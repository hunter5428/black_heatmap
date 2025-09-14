import pandas as pd
import logging
from typing import List, Optional
from pathlib import Path
from config.db_config import load_config
from db.oracle_connector import OracleConnector
from utils.excel_processor import ExcelProcessor

logger = logging.getLogger(__name__)

class BlackMidProcessor:
    """Black MID 정보 처리 클래스"""
    
    def __init__(self):
        self.oracle_config, _ = load_config()
        self.excel_processor = ExcelProcessor()
    
    def validate_mid_format(self, mid_list: List[str]) -> List[str]:
        """
        MID 형식 검증 (A로 시작하고 A로 끝나는 패턴)
        
        Args:
            mid_list: 검증할 MID 리스트
        
        Returns:
            검증된 MID 리스트
        """
        validated_list = []
        invalid_list = []
        
        for mid in mid_list:
            if mid.startswith('A') and mid.endswith('A'):
                validated_list.append(mid)
            else:
                invalid_list.append(mid)
        
        if invalid_list:
            logger.warning(f"유효하지 않은 MID 형식 {len(invalid_list)}개: {invalid_list[:5]}...")
        
        return validated_list
    
    def generate_query(self, mid_list: List[str]) -> str:
        """
        MID 리스트를 기반으로 고객 정보 조회 쿼리 생성
        
        Args:
            mid_list: 조회할 MID 리스트
        
        Returns:
            SQL 쿼리 문자열
        """
        # MID 리스트를 SQL IN 절 형식으로 변환
        mid_str = "', '".join(mid_list)
        
        query = f"""
        SELECT DISTINCT
            -- CID
            c.CUST_ID AS "CID",
            
            -- 이름(성명)
            c.CUST_KO_NM AS "이름",
            
            -- 성별
            (SELECT AML_DTL_CD_NM 
             FROM SM_CD_DTL 
             WHERE AML_COMN_CD = 'CUST_GNDR_CD' 
               AND AML_DTL_CD = c.CUST_GNDR_CD) AS "성별",
            
            -- 생년월일
            c.CUST_BDAY AS "생년월일",
            
            -- 고액자산가
            c.CLB_YN AS "고액자산가",
            
            -- 거주지 정보 (주소 + 상세주소)
            CASE 
                WHEN c.CUST_DTL_ADDR IS NOT NULL 
                THEN c.CUST_ADDR || ' ' || c.CUST_DTL_ADDR
                ELSE c.CUST_ADDR
            END AS "거주지정보",
            
            -- 직장명
            c.WPLC_NM AS "직장명",
            
            -- 직장 정보 (직장 주소 + 상세주소)
            CASE 
                WHEN c.WPLC_DTL_ADDR IS NOT NULL 
                THEN c.WPLC_ADDR || ' ' || c.WPLC_DTL_ADDR
                ELSE c.WPLC_ADDR
            END AS "직장정보",
            
            -- 핸드폰 번호 (연락처) - AES 복호화
            AES_DECRYPT(c.CUST_TEL_NO) AS "핸드폰번호",
            
            -- E-mail 주소 - AES 복호화
            AES_DECRYPT(c.CUST_EMAIL) AS "이메일주소",
            
            -- KYC 완료일시
            TO_CHAR(c.KYC_EXE_FNS_DTM, 'YYYY-MM-DD HH24:MI:SS') AS "KYC완료일시",
            
            -- MID 정보 추가
            COALESCE(m.MEM_ID, c.KYC_EXE_MEM_ID) AS "MID"
            
        FROM BTCAMLDB_OWN.KYC_CUST_BASE c
        
        -- MID 정보를 가진 회원 테이블과 조인
        LEFT JOIN BTCAMLDB_OWN.KYC_MEM_BASE m
            ON c.CUST_ID = m.CUST_ID
        
        WHERE 
            -- 개인 고객만 조회 (CUST_TYPE_CD = '01')
            c.CUST_TYPE_CD = '01'
            
            -- MID가 블랙리스트에 포함된 경우
            AND (
                m.MEM_ID IN ('{mid_str}')
                OR c.KYC_EXE_MEM_ID IN ('{mid_str}')
            )
        
        ORDER BY c.CUST_ID
        """
        
        return query
    
    def fetch_customer_info(self, mid_list: List[str], batch_size: int = 1000) -> pd.DataFrame:
        """
        MID 리스트에 해당하는 고객 정보를 조회
        
        Args:
            mid_list: 조회할 MID 리스트
            batch_size: 배치 크기
        
        Returns:
            고객 정보 DataFrame
        """
        if not mid_list:
            logger.warning("조회할 MID가 없습니다.")
            return pd.DataFrame()
        
        all_data = []
        
        with OracleConnector(self.oracle_config) as oracle_db:
            # 배치 처리
            for i in range(0, len(mid_list), batch_size):
                batch_mid_list = mid_list[i:i+batch_size]
                query = self.generate_query(batch_mid_list)
                
                logger.info(f"배치 {i//batch_size + 1} 조회 중... ({len(batch_mid_list)}개 MID)")
                
                try:
                    batch_df = oracle_db.execute_query(query)
                    all_data.append(batch_df)
                    logger.info(f"배치 {i//batch_size + 1} 완료: {len(batch_df)}건 조회됨")
                except Exception as e:
                    logger.error(f"배치 {i//batch_size + 1} 조회 실패: {str(e)}")
                    continue
        
        # 모든 배치 결과 합치기
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (동일한 CID가 여러 MID를 가질 수 있음)
            df = df.drop_duplicates(subset=['CID'], keep='first')
            
            # 컬럼 순서 정리
            column_order = [
                'CID', '이름', '성별', '생년월일', '고액자산가',
                '거주지정보', '직장명', '직장정보', '핸드폰번호',
                '이메일주소', 'KYC완료일시', 'MID'
            ]
            
            # 존재하는 컬럼만 선택
            existing_columns = [col for col in column_order if col in df.columns]
            df = df[existing_columns]
            
            return df
        else:
            return pd.DataFrame()
    
    def process(self, xlsx_file_path: str, validate_format: bool = True) -> pd.DataFrame:
        """
        전체 처리 프로세스 실행
        
        Args:
            xlsx_file_path: Excel 파일 경로
            validate_format: MID 형식 검증 여부
        
        Returns:
            처리된 DataFrame
        """
        try:
            # 1. Excel에서 MID 리스트 읽기
            logger.info(f"Excel 파일 읽기 시작: {xlsx_file_path}")
            black_mid_list = self.excel_processor.read_mid_list(xlsx_file_path)
            
            if not black_mid_list:
                logger.warning("읽어온 MID가 없습니다.")
                return pd.DataFrame()
            
            logger.info(f"총 {len(black_mid_list)}개의 MID를 읽었습니다.")
            
            # 2. MID 형식 검증 (선택적)
            if validate_format:
                black_mid_list = self.validate_mid_format(black_mid_list)
                if not black_mid_list:
                    logger.warning("유효한 MID가 없습니다.")
                    return pd.DataFrame()
            
            # 3. 고객 정보 조회
            logger.info(f"{len(black_mid_list)}개의 MID 조회 시작")
            df_black_info = self.fetch_customer_info(black_mid_list)
            
            if not df_black_info.empty:
                logger.info(f"최종 조회 완료: 총 {len(df_black_info)}명의 고객 정보")
                
                # 조회되지 않은 MID 확인
                if 'MID' in df_black_info.columns:
                    found_mids = df_black_info['MID'].unique().tolist()
                    not_found_mids = list(set(black_mid_list) - set(found_mids))
                    if not_found_mids:
                        logger.warning(f"조회되지 않은 MID {len(not_found_mids)}개")
                        logger.debug(f"조회되지 않은 MID 목록: {not_found_mids[:10]}...")
            else:
                logger.warning("조회된 데이터가 없습니다.")
            
            return df_black_info
            
        except Exception as e:
            logger.error(f"Black MID 처리 실패: {str(e)}")
            raise