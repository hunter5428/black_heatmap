import psycopg2
import pandas as pd
from typing import Optional
import logging
from .base_connector import BaseDBConnector
from config.db_config import RedshiftConfig

logger = logging.getLogger(__name__)

class RedshiftConnector(BaseDBConnector):
    """Redshift 데이터베이스 커넥터"""
    
    def __init__(self, config: RedshiftConfig):
        super().__init__()
        self.config = config
    
    def connect(self) -> None:
        """Redshift 데이터베이스 연결"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            self.cursor = self.connection.cursor()
            
            # 스키마 설정 (필요한 경우)
            if self.config.schema:
                self.cursor.execute(f"SET search_path TO {self.config.schema}")
                
            logger.info("Redshift DB 연결 성공")
        except Exception as e:
            logger.error(f"Redshift DB 연결 실패: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Redshift 데이터베이스 연결 해제"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Redshift DB 연결 해제")
        except Exception as e:
            logger.error(f"Redshift DB 연결 해제 실패: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """쿼리 실행 및 DataFrame 반환"""
        try:
            # pandas read_sql 사용 (더 효율적)
            df = pd.read_sql_query(
                query, 
                self.connection, 
                params=params
            )
            logger.info(f"쿼리 실행 성공: {len(df)} rows fetched")
            
            return df
            
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {str(e)}")
            raise