# Black Heatmap 데이터 처리 시스템

## 설치 방법

### 1. 패키지 설치
```bash
pip install jaydebeapi==1.2.3 psycopg2-binary pandas openpyxl xlwings python-dotenv pyyaml plotly numpy
```

### 2. Oracle JDBC 드라이버 설정
- ojdbc11-21.5.0.0.jar 파일을 다운로드하여 프로젝트 폴더에 저장
- .env 파일에서 ORACLE_DRIVER_PATH 경로 설정

### 3. 환경 변수 설정 (.env 파일)
프로젝트 루트에 `.env` 파일을 생성하고 아래 내용 입력:

```
# Oracle 설정
ORACLE_HOST=127.0.0.1
ORACLE_PORT=40112
ORACLE_SERVICE_NAME=your_service_name
ORACLE_USERNAME=your_oracle_username
ORACLE_PASSWORD=your_oracle_password
ORACLE_DRIVER_PATH=C:\ojdbc11-21.5.0.0.jar

# Redshift 설정
REDSHIFT_HOST=127.0.0.1
REDSHIFT_PORT=40127
REDSHIFT_DATABASE=prod
REDSHIFT_USERNAME=your_redshift_username
REDSHIFT_PASSWORD=your_redshift_password
```

### 4. 프로그램 실행
```bash
python main.py
```

## 주요 기능

1. **통합 데이터 처리**: Oracle과 Redshift에서 데이터를 조회하여 통합
2. **Oracle 전용 처리**: Black MID 고객 정보 조회
3. **사용자 정의 쿼리**: SQL 파일을 통한 커스텀 쿼리 실행
4. **데이터 시각화**: 거래 데이터 히트맵, 타임라인 차트 등 생성

## 프로젝트 구조

```
black-heatmap/
├── config/           # 데이터베이스 설정
├── db/              # 데이터베이스 커넥터
├── processors/      # 데이터 처리 모듈
├── utils/           # 유틸리티 (Excel, 시각화 등)
├── query/           # SQL 쿼리 파일
│   ├── oracledb/
│   └── redshift/
├── logs/            # 로그 파일 (자동 생성)
├── output/          # 출력 파일 (자동 생성)
└── main.py          # 메인 실행 파일
```