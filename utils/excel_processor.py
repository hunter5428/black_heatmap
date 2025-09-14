import xlwings as xw
import pandas as pd
import logging
from typing import List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class ExcelProcessor:
    """Excel 파일 처리 유틸리티"""
    
    @staticmethod
    def read_mid_list(xlsx_file_path: str, column: str = 'A', header_row: int = 1) -> List[str]:
        """
        Excel 파일에서 MID 리스트 읽기
        
        Args:
            xlsx_file_path: Excel 파일 경로
            column: 읽을 컬럼 (기본값: 'A')
            header_row: 헤더 행 번호 (기본값: 1)
        
        Returns:
            MID 리스트
        """
        try:
            # 파일 존재 확인
            if not Path(xlsx_file_path).exists():
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {xlsx_file_path}")
            
            # xlwings로 백그라운드에서 Excel 파일 열기
            app = xw.App(visible=False, add_book=False)
            try:
                wb = app.books.open(xlsx_file_path)
                ws = wb.sheets[0]  # 첫 번째 시트 선택
                
                # 데이터 범위 확인
                last_row = ws.range(f'{column}{header_row}').end('down').row
                
                # 데이터 읽기
                if last_row > header_row:
                    data_range = f'{column}{header_row + 1}:{column}{last_row}'
                    mid_list = ws.range(data_range).value
                    
                    # 단일 값인 경우 리스트로 변환
                    if not isinstance(mid_list, list):
                        mid_list = [mid_list]
                    
                    # None 값 제거 및 문자열 변환
                    mid_list = [str(mid).strip() for mid in mid_list 
                               if mid is not None and str(mid).strip()]
                    
                    logger.info(f"Excel에서 {len(mid_list)}개의 항목을 읽었습니다.")
                    return mid_list
                else:
                    logger.warning("Excel 파일에 데이터가 없습니다.")
                    return []
                    
            finally:
                wb.close()
                app.quit()
                
        except Exception as e:
            logger.error(f"Excel 파일 읽기 실패: {str(e)}")
            raise
    
    @staticmethod
    def save_dataframe(df: pd.DataFrame, output_path: str, sheet_name: str = 'Sheet1'):
        """
        DataFrame을 Excel 파일로 저장
        
        Args:
            df: 저장할 DataFrame
            output_path: 저장할 파일 경로
            sheet_name: 시트 이름
        """
        try:
            # 디렉토리 생성
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 컬럼 너비 자동 조정
                worksheet = writer.sheets[sheet_name]
                for column in df:
                    column_length = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    # Excel 컬럼 문자 계산 (A, B, C, ..., AA, AB, ...)
                    if col_idx < 26:
                        col_letter = chr(65 + col_idx)
                    else:
                        col_letter = chr(65 + col_idx // 26 - 1) + chr(65 + col_idx % 26)
                    worksheet.column_dimensions[col_letter].width = min(column_length + 2, 50)
            
            logger.info(f"Excel 파일 저장 완료: {output_path}")
        except Exception as e:
            logger.error(f"Excel 파일 저장 실패: {str(e)}")
            raise