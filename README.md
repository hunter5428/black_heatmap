# Windows에서 프로젝트 설치 및 실행 방법

## 1. 한 줄 설치 명령어 (PowerShell 또는 CMD에서 실행)
pip install jaydebeapi==1.2.3 psycopg2-binary pandas openpyxl xlwings python-dotenv pyyaml plotly numpy

## 2. 또는 requirements.txt 파일을 사용한 설치
pip install -r requirements.txt

## 3. Oracle JDBC 드라이버 설정
# ojdbc11-21.5.0.0.jar 파일을 다운로드하여 C:\ 드라이브에 저장
# 또는 .env 파일에서 ORACLE_DRIVER_PATH 경로 수정

## 4. 환경 변수 설정 (.env 파일 생성)
# 프로젝트 루트 디렉토리에 .env 파일을 생성하고 아래 내용 입력:

# Oracle 설정
ORACLE_HOST=127.0.0.1
ORACLE_PORT=40112
ORACLE_SERVICE_NAME=your_service_name
ORACLE_USERNAME=your_username
ORACLE_PASSWORD=your_password
ORACLE_DRIVER_PATH=C:\ojdbc11-21.5.0.0.jar

# Redshift 설정
REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_database
REDSHIFT_USERNAME=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_SCHEMA=public

## 5. 프로그램 실행
python main.py