import pandas as pd
import logging
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime
from processors.black_mid_processor import BlackMidProcessor
from processors.redshift_user_processor import RedshiftUserProcessor
from utils.excel_processor import ExcelProcessor

logger = logging.getLogger(__name__)

class IntegratedProcessor:
    """Oracle과 Redshift 데이터를 통합 처리하는 클래스"""
    
    def __init__(self):
        self.oracle_processor = BlackMidProcessor()
        self.redshift_processor = RedshiftUserProcessor()
        self.excel_processor = ExcelProcessor()
    
    def process_integrated_data(self, xlsx_file_path: str, checkpoint_datetime: str,
                               start_time: str, end_time: str,
                               validate_mid_format: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Oracle과 Redshift에서 데이터를 조회하여 통합
        
        Args:
            xlsx_file_path: Excel 파일 경로 (MID 리스트)
            checkpoint_datetime: Redshift 조회 시작 날짜 (YYYY-MM-DD)
            start_time: 거래 조회 시작 시간 (YYYY-MM-DD HH:MI:SS)
            end_time: 거래 조회 종료 시간 (YYYY-MM-DD HH:MI:SS)
            validate_mid_format: MID 형식 검증 여부
        
        Returns:
            통합된 DataFrame 딕셔너리
        """
        try:
            logger.info("="*50)
            logger.info("통합 데이터 처리 시작")
            logger.info("="*50)
            
            # 1. Oracle에서 Black MID 정보 조회
            logger.info("\n[1/2] Oracle DB에서 고객 정보 조회 중...")
            df_oracle = self.oracle_processor.process(xlsx_file_path, validate_mid_format)
            
            if df_oracle.empty:
                logger.warning("Oracle에서 조회된 데이터가 없습니다.")
                df_oracle = pd.DataFrame()
            else:
                logger.info(f"Oracle 조회 완료: {len(df_oracle)}건")
                # Oracle의 MID 컬럼명 통일
                if 'MID' in df_oracle.columns:
                    df_oracle = df_oracle.rename(columns={'MID': 'mid'})
            
            # 2. Redshift에서 사용자 정보 조회
            logger.info("\n[2/2] Redshift에서 사용자 정보 조회 중...")
            redshift_result = self.redshift_processor.process(
                xlsx_file_path, 
                checkpoint_datetime,
                start_time,
                end_time
            )
            
            # Redshift 결과 분리
            df_redshift_base = redshift_result.get('base_info', pd.DataFrame())
            df_1h_buysell_amountkrw = redshift_result.get('df_1h_buysell_amountkrw', pd.DataFrame())
            df_4h_buysell_amountkrw = redshift_result.get('df_4h_buysell_amountkrw', pd.DataFrame())
            df_day_buysell_info = redshift_result.get('df_day_buysell_info', pd.DataFrame())
            
            if df_redshift_base.empty:
                logger.warning("Redshift에서 조회된 기본 데이터가 없습니다.")
            else:
                logger.info(f"Redshift 기본 정보 조회 완료: {len(df_redshift_base)}건")
            
            if not df_1h_buysell_amountkrw.empty:
                logger.info(f"1시간 단위 거래 데이터 조회 완료: {len(df_1h_buysell_amountkrw)}건")
            
            if not df_4h_buysell_amountkrw.empty:
                logger.info(f"4시간 단위 거래 데이터 조회 완료: {len(df_4h_buysell_amountkrw)}건")
            
            if not df_day_buysell_info.empty:
                logger.info(f"일별 거래 상세 데이터 조회 완료: {len(df_day_buysell_info)}건")
            
            # 3. 기본 데이터 통합 (Oracle + Redshift 기본 정보)
            logger.info("\n데이터 통합 중...")
            
            if not df_oracle.empty and not df_redshift_base.empty:
                # mid를 기준으로 outer join
                df_black_mid_info = pd.merge(
                    df_oracle,
                    df_redshift_base,
                    on='mid',
                    how='outer',
                    suffixes=('_oracle', '_redshift')
                )
                logger.info(f"기본 데이터 통합 완료: Oracle({len(df_oracle)}건) + Redshift({len(df_redshift_base)}건) = 통합({len(df_black_mid_info)}건)")
                
            elif not df_oracle.empty:
                df_black_mid_info = df_oracle
                logger.info(f"Oracle 데이터만 사용: {len(df_black_mid_info)}건")
                
            elif not df_redshift_base.empty:
                df_black_mid_info = df_redshift_base
                logger.info(f"Redshift 데이터만 사용: {len(df_black_mid_info)}건")
                
            else:
                # Excel에서 MID 리스트만 읽어서 DataFrame 생성
                mid_list = self.excel_processor.read_mid_list(xlsx_file_path)
                df_black_mid_info = pd.DataFrame({'mid': mid_list})
                logger.warning("Oracle과 Redshift 모두 기본 데이터가 없습니다. MID 리스트만 반환합니다.")
            
            # 4. 컬럼 순서 정리 (mid를 첫 번째로)
            if 'mid' in df_black_mid_info.columns:
                cols = df_black_mid_info.columns.tolist()
                cols = ['mid'] + [col for col in cols if col != 'mid']
                df_black_mid_info = df_black_mid_info[cols]
            
            # 5. 중복 제거 (mid 기준)
            if not df_black_mid_info.empty:
                before_dedup = len(df_black_mid_info)
                df_black_mid_info = df_black_mid_info.drop_duplicates(subset=['mid'], keep='first')
                after_dedup = len(df_black_mid_info)
                if before_dedup != after_dedup:
                    logger.info(f"중복 제거: {before_dedup}건 → {after_dedup}건")
            
            # 6. 결과 요약
            logger.info("\n" + "="*50)
            logger.info("통합 처리 완료")
            logger.info(f"기본 정보: {len(df_black_mid_info)}건")
            logger.info(f"1시간 단위 거래: {len(df_1h_buysell_amountkrw)}건")
            logger.info(f"4시간 단위 거래: {len(df_4h_buysell_amountkrw)}건")
            logger.info(f"일별 거래 상세: {len(df_day_buysell_info)}건")
            logger.info("="*50)
            
            return {
                'df_black_mid_info': df_black_mid_info,
                'df_1h_buysell_amountkrw': df_1h_buysell_amountkrw,
                'df_4h_buysell_amountkrw': df_4h_buysell_amountkrw,
                'df_day_buysell_info': df_day_buysell_info
            }
            
        except Exception as e:
            logger.error(f"통합 데이터 처리 실패: {str(e)}")
            raise