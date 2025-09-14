import os
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class QueryLoader:
    """SQL 쿼리 파일 로더"""
    
    def __init__(self, base_path: str = "query"):
        self.base_path = base_path
        self.queries: Dict[str, str] = {}
    
    def load_query(self, db_type: str, query_name: str) -> str:
        """
        SQL 파일 로드
        
        Args:
            db_type: 'oracledb' 또는 'redshift'
            query_name: 쿼리 파일명 (확장자 제외)
        
        Returns:
            SQL 쿼리 문자열
        """
        file_path = os.path.join(
            self.base_path, 
            db_type, 
            f"{query_name}.sql"
        )
        
        # 캐시 확인
        cache_key = f"{db_type}_{query_name}"
        if cache_key in self.queries:
            return self.queries[cache_key]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                query = f.read()
                self.queries[cache_key] = query
                logger.info(f"쿼리 로드 성공: {file_path}")
                return query
        except FileNotFoundError:
            logger.error(f"쿼리 파일을 찾을 수 없음: {file_path}")
            raise
        except Exception as e:
            logger.error(f"쿼리 로드 실패: {str(e)}")
            raise
    
    def load_all_queries(self, db_type: str) -> Dict[str, str]:
        """
        특정 DB 타입의 모든 쿼리 로드
        
        Args:
            db_type: 'oracledb' 또는 'redshift'
        
        Returns:
            쿼리 이름과 SQL 문자열의 딕셔너리
        """
        queries = {}
        db_path = os.path.join(self.base_path, db_type)
        
        if not os.path.exists(db_path):
            logger.warning(f"쿼리 디렉토리를 찾을 수 없음: {db_path}")
            return queries
        
        for filename in os.listdir(db_path):
            if filename.endswith('.sql'):
                query_name = filename[:-4]  # .sql 제거
                queries[query_name] = self.load_query(db_type, query_name)
        
        return queries