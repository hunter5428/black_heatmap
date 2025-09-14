import jaydebeapi
import pandas as pd
from typing import Optional
import logging
from .base_connector import BaseDBConnector
from config.db_config import OracleConfig

logger = logging.getLogger(__name__)

class OracleConnector(BaseDBConnector):
    """Oracle 데이터베이스 커넥터"""
    
    def __init__(self, config: OracleConfig):
        super().__init__()
        self.config = config
        
    def connect(self) -> None:
        """Oracle 데이터베이스 연결"""
        try:
            self.connection = jaydebeapi.connect(
                self.config.driver_class,
                self.config.jdbc_url,
                [self.config.username, self.config.password],
                self.config.driver_path
            )
            self.cursor = self.connection.cursor()
            logger.info("Oracle DB 연결 성공")
        except Exception as e:
            logger.error(f"Oracle DB 연결 실패: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Oracle 데이터베이스 연결 해제"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Oracle DB 연결 해제")
        except Exception as e:
            logger.error(f"Oracle DB 연결 해제 실패: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """쿼리 실행 및 DataFrame 반환"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # 컬럼명 가져오기
            columns = [desc[0] for desc in self.cursor.description]
            
            # 데이터 가져오기
            data = self.cursor.fetchall()
            
            # DataFrame 생성
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"쿼리 실행 성공: {len(df)} rows fetched")
            
            return df
            
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {str(e)}")
            raise