import pandas as pd
import logging
from typing import List, Optional, Dict
from datetime import datetime
from config.db_config import load_config
from db.redshift_connector import RedshiftConnector
from utils.query_loader import QueryLoader
from utils.excel_processor import ExcelProcessor

logger = logging.getLogger(__name__)

class RedshiftUserProcessor:
    """Redshift 사용자 정보 처리 클래스"""
    
    def __init__(self):
        _, self.redshift_config = load_config()
        self.query_loader = QueryLoader()
        self.excel_processor = ExcelProcessor()
    
    def prepare_query_with_params(self, query_name: str, user_ids: List[str], 
                                 checkpoint_datetime: Optional[str] = None,
                                 start_time: Optional[str] = None,
                                 end_time: Optional[str] = None) -> str:
        """
        쿼리 파라미터 치환
        
        Args:
            query_name: 쿼리 파일명
            user_ids: 사용자 ID 리스트
            checkpoint_datetime: 체크포인트 일자 (YYYY-MM-DD)
            start_time: 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 종료 시간 (YYYY-MM-DD HH:MI:SS)
        
        Returns:
            파라미터가 치환된 SQL 쿼리
        """
        # 쿼리 파일 로드
        base_query = self.query_loader.load_query('redshift', query_name)
        
        # user_ids 파라미터 치환
        user_ids_str = "','".join(user_ids)
        user_ids_str = f"'{user_ids_str}'"
        query = base_query.replace(':user_ids', user_ids_str)
        
        # checkpoint_datetime 파라미터 치환
        if checkpoint_datetime and ':checkpoint_datetime' in query:
            query = query.replace(':checkpoint_datetime', f"'{checkpoint_datetime}'")
        
        # start_time 파라미터 치환
        if start_time and ':start_time' in query:
            query = query.replace(':start_time', f"'{start_time}'")
        
        # end_time 파라미터 치환
        if end_time and ':end_time' in query:
            query = query.replace(':end_time', f"'{end_time}'")
        
        return query
    
    def get_user_access_info(self, user_id_list: List[str], checkpoint_datetime: str) -> pd.DataFrame:
        """
        사용자 접속 정보 조회
        
        Args:
            user_id_list: 사용자 ID 리스트
            checkpoint_datetime: 조회 시작 일자 (YYYY-MM-DD)
        
        Returns:
            접속 정보 DataFrame
        """
        try:
            with RedshiftConnector(self.redshift_config) as connector:
                query = self.prepare_query_with_params(
                    'user_access_info', 
                    user_id_list, 
                    checkpoint_datetime=checkpoint_datetime
                )
                df = connector.execute_query(query)
                logger.info(f"사용자 접속 정보 조회 완료: {len(df)}건")
                return df
        except Exception as e:
            logger.error(f"사용자 접속 정보 조회 실패: {str(e)}")
            return pd.DataFrame()
    
    def get_user_join_date(self, user_id_list: List[str]) -> pd.DataFrame:
        """
        사용자 가입일자 조회
        
        Args:
            user_id_list: 사용자 ID 리스트
        
        Returns:
            가입일자 정보 DataFrame
        """
        try:
            with RedshiftConnector(self.redshift_config) as connector:
                query = self.prepare_query_with_params('user_join_date', user_id_list)
                df = connector.execute_query(query)
                
                # 중복 제거
                if not df.empty:
                    df = df.drop_duplicates(subset=['user_id', 'join_datetime'], keep='first')
                    logger.info(f"사용자 가입일자 조회 완료: {len(df)}건")
                
                return df
        except Exception as e:
            logger.error(f"사용자 가입일자 조회 실패: {str(e)}")
            return pd.DataFrame()
    
    def get_1h_buysell_amount(self, user_id_list: List[str], start_time: str, end_time: str) -> pd.DataFrame:
        """
        1시간 단위 거래금액 집계 조회
        
        Args:
            user_id_list: 사용자 ID 리스트
            start_time: 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 종료 시간 (YYYY-MM-DD HH:MI:SS)
        
        Returns:
            1시간 단위 거래금액 DataFrame
        """
        try:
            with RedshiftConnector(self.redshift_config) as connector:
                query = self.prepare_query_with_params(
                    'orderbook_1h_summary',
                    user_id_list,
                    start_time=start_time,
                    end_time=end_time
                )
                df = connector.execute_query(query)
                logger.info(f"1시간 단위 거래금액 조회 완료: {len(df)}건")
                return df
        except Exception as e:
            logger.error(f"1시간 단위 거래금액 조회 실패: {str(e)}")
            return pd.DataFrame()
    
    def get_4h_buysell_amount(self, user_id_list: List[str], start_time: str, end_time: str) -> pd.DataFrame:
        """
        4시간 단위 거래금액 집계 조회
        
        Args:
            user_id_list: 사용자 ID 리스트
            start_time: 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 종료 시간 (YYYY-MM-DD HH:MI:SS)
        
        Returns:
            4시간 단위 거래금액 DataFrame
        """
        try:
            with RedshiftConnector(self.redshift_config) as connector:
                query = self.prepare_query_with_params(
                    'orderbook_4h_summary',
                    user_id_list,
                    start_time=start_time,
                    end_time=end_time
                )
                df = connector.execute_query(query)
                logger.info(f"4시간 단위 거래금액 조회 완료: {len(df)}건")
                return df
        except Exception as e:
            logger.error(f"4시간 단위 거래금액 조회 실패: {str(e)}")
            return pd.DataFrame()
    
    def get_daily_buysell_info(self, user_id_list: List[str], start_time: str, end_time: str) -> pd.DataFrame:
        """
        일별/종목별 거래 상세 조회
        
        Args:
            user_id_list: 사용자 ID 리스트
            start_time: 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 종료 시간 (YYYY-MM-DD HH:MI:SS)
        
        Returns:
            일별 거래 상세 DataFrame
        """
        try:
            with RedshiftConnector(self.redshift_config) as connector:
                query = self.prepare_query_with_params(
                    'orderbook_daily_detail',
                    user_id_list,
                    start_time=start_time,
                    end_time=end_time
                )
                df = connector.execute_query(query)
                logger.info(f"일별 거래 상세 조회 완료: {len(df)}건")
                return df
        except Exception as e:
            logger.error(f"일별 거래 상세 조회 실패: {str(e)}")
            return pd.DataFrame()
    
    def process(self, xlsx_file_path: str, checkpoint_datetime: str, 
                start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        전체 사용자 데이터 처리 프로세스
        
        Args:
            xlsx_file_path: Excel 파일 경로
            checkpoint_datetime: 조회 시작 일자 (YYYY-MM-DD)
            start_time: 거래 조회 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 거래 조회 종료 시간 (YYYY-MM-DD HH:MI:SS)
        
        Returns:
            처리 결과 딕셔너리
        """
        try:
            # 1. Excel에서 MID 리스트 읽기
            logger.info(f"Excel 파일 읽기: {xlsx_file_path}")
            mid_list = self.excel_processor.read_mid_list(xlsx_file_path)
            
            if not mid_list:
                logger.warning("읽어온 MID가 없습니다.")
                return {}
            
            logger.info(f"총 {len(mid_list)}개의 MID를 읽었습니다.")
            
            # 2. 사용자 접속 정보 조회
            df_access_info = self.get_user_access_info(mid_list, checkpoint_datetime)
            
            # 3. 사용자 가입일자 조회
            df_join_date = self.get_user_join_date(mid_list)
            
            # 4. 거래 데이터 조회 (start_time과 end_time이 제공된 경우)
            df_1h_buysell_amountkrw = pd.DataFrame()
            df_4h_buysell_amountkrw = pd.DataFrame()
            df_day_buysell_info = pd.DataFrame()
            
            if start_time and end_time:
                df_1h_buysell_amountkrw = self.get_1h_buysell_amount(mid_list, start_time, end_time)
                df_4h_buysell_amountkrw = self.get_4h_buysell_amount(mid_list, start_time, end_time)
                df_day_buysell_info = self.get_daily_buysell_info(mid_list, start_time, end_time)
            
            # 5. 기본 정보 병합 (접속 정보 + 가입일자)
            if not df_access_info.empty and not df_join_date.empty:
                df_base = pd.merge(
                    df_join_date,
                    df_access_info,
                    on='user_id',
                    how='outer'
                )
            elif not df_join_date.empty:
                df_base = df_join_date
            elif not df_access_info.empty:
                df_base = df_access_info
            else:
                df_base = pd.DataFrame({'user_id': mid_list})
            
            # 6. 컬럼명 정리 (user_id를 mid로 변경)
            if 'user_id' in df_base.columns:
                df_base = df_base.rename(columns={'user_id': 'mid'})
            
            # 거래 데이터에서도 user_id를 mid로 변경
            if not df_1h_buysell_amountkrw.empty and 'user_id' in df_1h_buysell_amountkrw.columns:
                df_1h_buysell_amountkrw = df_1h_buysell_amountkrw.rename(columns={'user_id': 'mid'})
            
            if not df_4h_buysell_amountkrw.empty and 'user_id' in df_4h_buysell_amountkrw.columns:
                df_4h_buysell_amountkrw = df_4h_buysell_amountkrw.rename(columns={'user_id': 'mid'})
            
            if not df_day_buysell_info.empty and 'user_id' in df_day_buysell_info.columns:
                df_day_buysell_info = df_day_buysell_info.rename(columns={'user_id': 'mid'})
            
            logger.info(f"Redshift 데이터 처리 완료")
            
            return {
                'base_info': df_base,
                'df_1h_buysell_amountkrw': df_1h_buysell_amountkrw,
                'df_4h_buysell_amountkrw': df_4h_buysell_amountkrw,
                'df_day_buysell_info': df_day_buysell_info
            }
            
        except Exception as e:
            logger.error(f"사용자 데이터 처리 실패: {str(e)}")
            return {}