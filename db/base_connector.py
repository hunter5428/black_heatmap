from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)

class BaseDBConnector(ABC):
    """데이터베이스 커넥터 기본 클래스"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    @abstractmethod
    def connect(self) -> None:
        """데이터베이스 연결"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """데이터베이스 연결 해제"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """쿼리 실행 및 DataFrame 반환"""
        pass
    
    def __enter__(self):
        """Context manager 진입"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 종료"""
        self.disconnect()