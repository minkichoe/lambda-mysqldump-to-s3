"""
데이터베이스 백업 스크립트

백업이 필요한 데이터베이스 정보를 규칙에 따라 저장, 관리합니다.


Author:
    최민기(minki.developer@gmail.com)
    
Rules:
    1. 최근 1주일: 하루 24개 (1 day = 24 backup files in the last week)
    2. 최근 1개월: 하루 1개 (1 day = 1 backup file in the last month)
    3. 그 이후: 일주일 중 특정 요일에 1개 저장 (1 week = 1 backup file over a month)

Used:
    python 3.7
    gzip 1.6
    mysqldump 5.7.29
    AWS Lambda(ap-northeast-2)
    AWS Cloudwatch event(ap-northeast-2)
    AWS IAM(ap-northeast-2)
    AWS S3(ap-northeast-2)
    
Envs:
    ACCESS_KEY: S3에 접근 할 IAM 공개키 (IAM Public Key to access to S3)
    SECRET_KEY: S3에 접근 할 IAM 비밀키 (IAM Secret Key to access to S3)
    JANDI_API_URL: 백업 실패시 기록할 Jandi API URL (optional)
    BUCKET_NAME: 저장될 S3 버킷명 (S3 bucket name)
    TEMP_BASE_DIR: 람다 내 백업파일이 임시저장 될 base path (optional)
    DATABASE_LIST: 백업대상 DB 및 테이블 정보(JSON) (backup databases)
    BACKUP_DAYS_OF_THE_WEEK: Rules 3번에 따른 지정 요일 (day of the week by Rules no.3)
"""

import os
import json
import logging
import subprocess
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pytz import timezone, utc
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# typing options
Path = str

# 전역 환경변수 참조
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
JANDI_API_URL = os.getenv('JANDI_API_URL')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'matchpoint-database-backup')
TEMP_BASE_DIR = os.getenv('TEMP_BASE_DIR', '/tmp')
DATABASE_LIST = json.loads(os.getenv('DATABASE_LIST'))
BACKUP_DAYS_OF_THE_WEEK = os.getenv('BACKUP_DAYS_OF_THE_WEEK', '일')

KST = timezone('Asia/Seoul')
PATH_TEMPLATE = '{base_dir}/{db_host}/{db_name}/{file}'
DAYS_OF_THE_WEEK = ['월', '화', '수', '목', '금', '토', '일']

s3_client = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)
s3_resource = boto3.resource('s3')
s3_bucket = s3_resource.Bucket(BUCKET_NAME)
    
def error_report(body: str, **messages: dict) -> int:
    """ Jandi incomming call API 호출.

    Jandi incomming call API를 호출하여 스쿨맘의 특정 채팅방에 에러사실을 공유합니다.
    
    https://support.jandi.com/hc/ko/articles/210952203-%EC%9E%94%EB%94%94-%EC%BB%A4%EB%84%A5%ED%8A%B8-%EC%9D%B8%EC%BB%A4%EB%B0%8D-%EC%9B%B9%ED%9B%85-Incoming-Webhook-%EC%9C%BC%EB%A1%9C-%EC%99%B8%EB%B6%80-%EB%8D%B0%EC%9D%B4%ED%84%B0%EB%A5%BC-%EC%9E%94%EB%94%94-%EB%A9%94%EC%8B%9C%EC%A7%80%EB%A1%9C-%EC%88%98%EC%8B%A0%ED%95%98%EA%B8%B0
    
    Args:
        body (str): 메시지 제목
        messages (dict): 메시지 키:값 목록

    Returns:
        int: HTTP Response 상태코드 반환

    Raises:
        urllib.error.URLError
    """
    data = {'body': body}
    data['connectInfo'] = [
        {'title': str(title), 'description': str(description)}
        for title, description in messages.items()
    ]

    request = Request(
        JANDI_API_URL,
        data=json.dumps(data).encode(),
        headers={
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.tosslab.jandi-v2+json',
        }
    )

    response = urlopen(request)
    return response.getcode()

def get_now(timezone=KST) -> str:
    """ 특정 지역의 현재 datetime을 문자열로 반환.
    
    Args:
        timezone (timezone): 지역 객체
    
    Returns:
        str: YYYY-MM-DD_HH:MM:SS 형식의 특정 지역 현재 datetime
    """
    now = datetime.utcnow()
    local_time = utc.localize(now).astimezone(timezone)
    return local_time.strftime('%Y-%m-%d_%H:%M:%S')

def save_file_to_local(db: dict, now: str, compress: bool=True) -> Path:
    """ lambda 로컬에 db dump파일을 임시 저장.
    
    Args:
        db (dict): DB 접속정보
        compress (bool): dump 파일 압축 유무
    
    Returns:
        Path (str): lambda 로컬에 저장된 db dump 파일의 절대경로
    """
    exp = 'gz' if compress else 'sql'
    
    tables_string = ' '.join(db["tables"])
    target_db_string = f'-h {db["host"]} --port {db["port"]} -u {db["username"]} -p{db["password"]} {db["name"]} {tables_string}'
    
    # 임시저장 경로 생성
    target_local_path = PATH_TEMPLATE.format(
        base_dir=TEMP_BASE_DIR,
        db_host=db["host"],
        db_name=db["name"],
        file=f'{now}.{exp}'
    )
    os.makedirs(os.path.dirname(target_local_path), exist_ok=True)

    # mysqldump, gzip(선택) 스크립트 생성
    mysqldump_string = f'/opt/bin/mysqldump --single-transaction {target_db_string}'
    command = f'{mysqldump_string} | gzip -9 > {target_local_path};' if compress else f'{mysqldump_string} > {target_local_path};'
    
    response = subprocess.run(command, shell=True, capture_output=True)
    
    return target_local_path
    
def backup(now: str) -> bool:
    """ DB dump 파일을 S3에 업로드
    
    DB dump 파일을 압축 및 임시저장 후 S3에 업로드 한 뒤, 임시저장 파일을 삭제합니다.

    Args:
        now(str): datetime 문자열
        
    Returns:
        bool: 성공시 True
    """
    for db in DATABASE_LIST:
        local_path = save_file_to_local(db, now)
        s3_path = local_path.replace(TEMP_BASE_DIR, '')
        response = s3_client.upload_file(
            Bucket=BUCKET_NAME,
            Filename=local_path,
            Key=s3_path
        )
        subprocess.run(f'rm -rf {TEMP_BASE_DIR}/*', shell=True)
    return True

def clean_up(now: str) -> bool:
    """ 파일을 Rules에 따라 삭제 
    
    Args:
        now(str): datetime 문자열
        
    Returns:
        bool: 성공시 True
    """
    def delete_keys_last_weeks(filename_prefix: str, db: dict):
        """ 특정일의 백업중 한 개만 남기고 모두 삭제
        
        Args:
            filename_prefix(str): 파일명 접두사
            db(dict): DB 커넥션 정보
        
        Returns:
            bool: 성공시 True
        """
        prefix = PATH_TEMPLATE.format(
            base_dir='',
            db_host=db['host'],
            db_name=db['name'],
            file=filename_prefix
        )
        file_objects = s3_bucket.objects.filter(Prefix=prefix)
        
        for hour, file_object in enumerate(file_objects):
            if hour is 0:
                continue
            else:
                file_object.delete()
                
        return True

    def delete_keys_last_months(filename_prefix: str, db: dict, day_of_the_week: str=BACKUP_DAYS_OF_THE_WEEK):
        """ 특정 월에서 특정요일 백업만 남기고 삭제
        
            Args:
                filename_prefix(str): 파일명 접두사
                db(dict): DB 커넥션 정보
                day_of_the_week(str): 특정요일, default: '일'
            
            Returns:
                bool: 성공시 True
        """
        
        prefix = PATH_TEMPLATE.format(
            base_dir='',
            db_host=db['host'],
            db_name=db['name'],
            file=filename_prefix
        )
        file_objects = s3_bucket.objects.filter(Prefix=prefix)
        
        for file_object in file_objects:
            # 매 주 한 요일 것만 남기고 삭제
            __, filename_with_extension = os.path.split(file_object.key)
            filename, __ = os.path.splitext(filename_with_extension)
            _date, time = filename.split('_')
            date = datetime.strptime(_date, "%Y-%m-%d").date()
            day_id = date.weekday()
            
            if DAYS_OF_THE_WEEK[day_id] == day_of_the_week:
                continue
            
            __ = file_object.delete()
            
        return True
            
    now_object = datetime.strptime(now, '%Y-%m-%d_%H:%M:%S').date()
    a_weeks_ago = now_object - timedelta(weeks=1)
    a_month_ago = now_object - timedelta(weeks=4)
    weeks_prefix = a_weeks_ago.strftime('%Y-%m-%d')
    month_prefix = a_month_ago.strftime('%Y-%m-')
    
    for db in DATABASE_LIST:
        __ = delete_keys_last_weeks(weeks_prefix, db)
        __ = delete_keys_last_months(month_prefix, db)
    
    return True

def lambda_handler(event, context):
    now = get_now()
    try:
        is_uploaded = backup(now)
        is_cleaned = clean_up(now)
    except Exception as exc:
        __ = error_report("DB 백업관리 실패", now=now, exc=exc, event=event, context=context)
        raise exc
    else:
        return {'statusCode': 200, 'body': json.dumps("success")}
    