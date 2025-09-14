#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from config.db_config import load_config
from db.oracle_connector import OracleConnector
from db.redshift_connector import RedshiftConnector
from utils.query_loader import QueryLoader
from utils.excel_processor import ExcelProcessor
from processors.black_mid_processor import BlackMidProcessor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/black_heatmap_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_black_mid_data():
    """Black MID 데이터 처리"""
    try:
        # Black MID 프로세서 초기화
        processor = BlackMidProcessor()
        excel_processor = ExcelProcessor()
        
        # Excel 파일 경로 설정
        xlsx_file_path = input("Black MID Excel 파일 경로를 입력하세요: ").strip()
        if not Path(xlsx_file_path).exists():
            logger.error(f"파일을 찾을 수 없습니다: {xlsx_file_path}")
            return None
        
        # 데이터 처리
        df_black_info = processor.process(xlsx_file_path, validate_format=True)
        
        if not df_black_info.empty:
            # 결과 출력
            print("\n" + "="*50)
            print("조회 결과 요약")
            print("="*50)
            print(f"총 조회된 고객 수: {len(df_black_info)}")
            print(f"\n컬럼 정보:")
            print(df_black_info.info())
            print(f"\n첫 5행 미리보기:")
            print(df_black_info.head())
            
            # 결과 저장 여부 확인
            save_result = input("\n결과를 Excel 파일로 저장하시겠습니까? (y/n): ").lower()
            if save_result == 'y':
                output_dir = Path('output')
                output_dir.mkdir(exist_ok=True)
                output_path = output_dir / f'black_mid_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                excel_processor.save_dataframe(df_black_info, str(output_path), 'Black_MID_Info')
                print(f"결과가 저장되었습니다: {output_path}")
            
            return df_black_info
        else:
            print("조회된 데이터가 없습니다.")
            return None
            
    except Exception as e:
        logger.error(f"Black MID 처리 중 오류 발생: {str(e)}")
        return None

def run_custom_query():
    """사용자 정의 쿼리 실행"""
    try:
        # 설정 로드
        oracle_config, redshift_config = load_config()
        query_loader = QueryLoader()
        excel_processor = ExcelProcessor()
        
        # DB 선택
        print("\n데이터베이스 선택:")
        print("1. Oracle")
        print("2. Redshift")
        db_choice = input("선택 (1 또는 2): ").strip()
        
        if db_choice == '1':
            db_type = 'oracledb'
            connector = OracleConnector(oracle_config)
        elif db_choice == '2':
            db_type = 'redshift'
            connector = RedshiftConnector(redshift_config)
        else:
            print("잘못된 선택입니다.")
            return None
        
        # 쿼리 파일 목록 표시
        queries = query_loader.load_all_queries(db_type)
        if queries:
            print(f"\n사용 가능한 쿼리 파일:")
            for i, query_name in enumerate(queries.keys(), 1):
                print(f"{i}. {query_name}")
            
            query_idx = int(input("실행할 쿼리 번호를 선택하세요: ")) - 1
            query_name = list(queries.keys())[query_idx]
            
            # 쿼리 실행
            with connector as db:
                query = queries[query_name]
                
                # 파라미터가 필요한 경우
                if ':' in query:
                    print("\n쿼리에 파라미터가 필요합니다.")
                    params = input("파라미터 값을 입력하세요 (쉼표로 구분): ").split(',')
                    df = db.execute_query(query, tuple(params))
                else:
                    df = db.execute_query(query)
                
                if not df.empty:
                    print(f"\n결과: {len(df)}개 행 조회됨")
                    print(df.head())
                    
                    # 저장 여부 확인
                    save_result = input("\n결과를 Excel로 저장하시겠습니까? (y/n): ").lower()
                    if save_result == 'y':
                        output_dir = Path('output')
                        output_dir.mkdir(exist_ok=True)
                        output_path = output_dir / f'{query_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                        excel_processor.save_dataframe(df, str(output_path))
                        print(f"저장 완료: {output_path}")
                    
                    return df
        else:
            print(f"{db_type}에 사용 가능한 쿼리가 없습니다.")
            return None
            
    except Exception as e:
        logger.error(f"쿼리 실행 중 오류: {str(e)}")
        return None

def main():
    """메인 실행 함수"""
    # 로그 디렉토리 생성
    Path('logs').mkdir(exist_ok=True)
    
    print("\n" + "="*50)
    print("Black Heatmap 데이터 처리 시스템")
    print("="*50)
    
    while True:
        print("\n메뉴:")
        print("1. Black MID 데이터 처리")
        print("2. 사용자 정의 쿼리 실행")
        print("3. 종료")
        
        choice = input("\n선택: ").strip()
        
        if choice == '1':
            df = process_black_mid_data()
        elif choice == '2':
            df = run_custom_query()
        elif choice == '3':
            print("프로그램을 종료합니다.")
            break
        else:
            print("잘못된 선택입니다. 다시 시도해주세요.")
    
    logger.info("프로그램 종료")

if __name__ == "__main__":
    main()