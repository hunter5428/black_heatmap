import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

@dataclass
class OracleConfig:
    """Oracle 데이터베이스 설정"""
    host: str = ""
    port: int = 40112
    service_name: str = ""
    username: str = ""
    password: str = ""
    driver_path: str = ""
    driver_class: str = "oracle.jdbc.driver.OracleDriver"
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"

@dataclass
class RedshiftConfig:
    """Redshift 데이터베이스 설정"""
    host: str = ""
    port: int = 5439
    database: str = ""
    username: str = ""
    password: str = ""
    schema: Optional[str] = "public"

def load_config():
    """환경변수에서 데이터베이스 설정 로드"""
    
    # Oracle 설정
    oracle_config = OracleConfig(
        host=os.getenv('ORACLE_HOST', '127.0.0.1'),
        port=int(os.getenv('ORACLE_PORT', '40112')),
        service_name=os.getenv('ORACLE_SERVICE_NAME', ''),
        username=os.getenv('ORACLE_USERNAME', ''),
        password=os.getenv('ORACLE_PASSWORD', ''),
        driver_path=os.getenv('ORACLE_DRIVER_PATH', r'C:\ojdbc11-21.5.0.0.jar')
    )
    
    # Redshift 설정
    redshift_config = RedshiftConfig(
        host=os.getenv('REDSHIFT_HOST', ''),
        port=int(os.getenv('REDSHIFT_PORT', '5439')),
        database=os.getenv('REDSHIFT_DATABASE', ''),
        username=os.getenv('REDSHIFT_USERNAME', ''),
        password=os.getenv('REDSHIFT_PASSWORD', ''),
        schema=os.getenv('REDSHIFT_SCHEMA', 'public')
    )
    
    # 설정 검증
    validate_config(oracle_config, redshift_config)
    
    return oracle_config, redshift_config

def validate_config(oracle_config: OracleConfig, redshift_config: RedshiftConfig):
    """필수 설정 값 검증"""
    
    # Oracle 필수 설정 확인
    oracle_required = ['host', 'service_name', 'username', 'password', 'driver_path']
    oracle_missing = []
    for field in oracle_required:
        if not getattr(oracle_config, field):
            oracle_missing.append(field)
    
    if oracle_missing:
        print(f"Warning: Oracle 설정 누락 - {', '.join(oracle_missing)}")
    
    # Redshift 필수 설정 확인
    redshift_required = ['host', 'database', 'username', 'password']
    redshift_missing = []
    for field in redshift_required:
        if not getattr(redshift_config, field):
            redshift_missing.append(field)
    
    if redshift_missing:
        print(f"Warning: Redshift 설정 누락 - {', '.join(redshift_missing)}")