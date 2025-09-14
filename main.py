#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from config.db_config import load_config
from db.oracle_connector import OracleConnector
from db.redshift_connector import RedshiftConnector
from utils.query_loader import QueryLoader
from utils.excel_processor import ExcelProcessor
from processors.black_mid_processor import BlackMidProcessor
from processors.redshift_user_processor import RedshiftUserProcessor
from processors.integrated_processor import IntegratedProcessor

# 로그 디렉토리 생성
Path('logs').mkdir(exist_ok=True)

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

def validate_date_format(date_string: str) -> bool:
    """날짜 형식 검증 (YYYY-MM-DD)"""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_datetime_range():
    """거래 조회용 시작/종료 시간 입력받기"""
    while True:
        start_date = input("거래 조회 시작 날짜를 입력하세요 (YYYY-MM-DD): ").strip()
        if validate_date_format(start_date):
            break
        else:
            print("올바른 날짜 형식이 아닙니다. YYYY-MM-DD 형식으로 입력해주세요.")
    
    while True:
        end_date = input("거래 조회 종료 날짜를 입력하세요 (YYYY-MM-DD): ").strip()
        if validate_date_format(end_date):
            break
        else:
            print("올바른 날짜 형식이 아닙니다. YYYY-MM-DD 형식으로 입력해주세요.")
    
    # 시작 시간은 00:00:00, 종료 시간은 23:59:59로 설정
    start_time = f"{start_date} 00:00:00"
    end_time = f"{end_date} 23:59:59"
    
    return start_time, end_time

def process_integrated_black_mid():
    """Oracle과 Redshift 데이터 통합 처리"""
    try:
        # 통합 프로세서 초기화
        processor = IntegratedProcessor()
        excel_processor = ExcelProcessor()
        
        # Excel 파일 경로 설정
        xlsx_file_path = input("MID Excel 파일 경로를 입력하세요: ").strip()
        if not Path(xlsx_file_path).exists():
            logger.error(f"파일을 찾을 수 없습니다: {xlsx_file_path}")
            return None
        
        # 체크포인트 날짜 입력 (Redshift 접속 정보용)
        while True:
            checkpoint_date = input("Redshift 접속 정보 조회 시작 날짜를 입력하세요 (YYYY-MM-DD): ").strip()
            if validate_date_format(checkpoint_date):
                break
            else:
                print("올바른 날짜 형식이 아닙니다. YYYY-MM-DD 형식으로 입력해주세요.")
        
        # 거래 데이터 조회 기간 입력
        print("\n거래 데이터 조회 기간 설정")
        start_time, end_time = get_datetime_range()
        print(f"거래 조회 기간: {start_time} ~ {end_time}")
        
        # MID 형식 검증 여부 확인
        validate_format = input("\nMID 형식 검증을 수행하시겠습니까? (y/n, 기본값: y): ").lower()
        validate_format = validate_format != 'n'
        
        # 통합 데이터 처리
        result = processor.process_integrated_data(
            xlsx_file_path, 
            checkpoint_date,
            start_time,
            end_time,
            validate_format
        )
        
        df_black_mid_info = result.get('df_black_mid_info', pd.DataFrame())
        df_4h_buysell_amountkrw = result.get('df_4h_buysell_amountkrw', pd.DataFrame())
        df_day_buysell_info = result.get('df_day_buysell_info', pd.DataFrame())
        
        if not df_black_mid_info.empty:
            # 결과 출력
            print("\n" + "="*50)
            print("통합 조회 결과 요약")
            print("="*50)
            
            print(f"\n1. 기본 정보 (df_black_mid_info)")
            print(f"   - 총 MID 수: {len(df_black_mid_info)}")
            print(f"   - 총 컬럼 수: {len(df_black_mid_info.columns)}")
            print(f"   - 첫 5행 미리보기:")
            print(df_black_mid_info.head())
            
            if not df_4h_buysell_amountkrw.empty:
                print(f"\n2. 4시간 단위 거래 집계 (df_4h_buysell_amountkrw)")
                print(f"   - 총 레코드 수: {len(df_4h_buysell_amountkrw)}")
                print(f"   - 첫 5행 미리보기:")
                print(df_4h_buysell_amountkrw.head())
            
            if not df_day_buysell_info.empty:
                print(f"\n3. 일별 거래 상세 (df_day_buysell_info)")
                print(f"   - 총 레코드 수: {len(df_day_buysell_info)}")
                print(f"   - 첫 5행 미리보기:")
                print(df_day_buysell_info.head())
            
            # 결과 저장 여부 확인
            save_result = input("\n결과를 Excel 파일로 저장하시겠습니까? (y/n): ").lower()
            if save_result == 'y':
                output_dir = Path('output')
                output_dir.mkdir(exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # 각 DataFrame을 별도 파일로 저장
                # 1. 기본 정보
                output_path1 = output_dir / f'integrated_black_mid_info_{timestamp}.xlsx'
                excel_processor.save_dataframe(df_black_mid_info, str(output_path1), 'MID_Info')
                print(f"기본 정보 저장: {output_path1}")
                
                # 2. 4시간 단위 거래 집계
                if not df_4h_buysell_amountkrw.empty:
                    output_path2 = output_dir / f'4h_buysell_amount_{timestamp}.xlsx'
                    excel_processor.save_dataframe(df_4h_buysell_amountkrw, str(output_path2), '4H_Trading')
                    print(f"4시간 거래 집계 저장: {output_path2}")
                
                # 3. 일별 거래 상세
                if not df_day_buysell_info.empty:
                    output_path3 = output_dir / f'daily_buysell_info_{timestamp}.xlsx'
                    excel_processor.save_dataframe(df_day_buysell_info, str(output_path3), 'Daily_Trading')
                    print(f"일별 거래 상세 저장: {output_path3}")
            
            # 시각화 생성 여부 확인
            create_viz = input("\n거래 데이터 시각화를 생성하시겠습니까? (y/n): ").lower()
            if create_viz == 'y' and (not df_4h_buysell_amountkrw.empty or not df_day_buysell_info.empty):
                visualizer = TradingVisualizer()
                
                print("\n시각화 옵션:")
                print("1. 4시간 단위 거래 히트맵")
                print("2. 시간대별 거래 추이")
                print("3. 상위 거래자 순위")
                print("4. 일별 패턴 분석")
                print("5. 모든 시각화 생성")
                
                viz_choice = input("\n선택 (1-5): ").strip()
                
                viz_results = {}
                
                if viz_choice == '1' and not df_4h_buysell_amountkrw.empty:
                    viz_results['heatmap'] = visualizer.create_4h_heatmap(df_4h_buysell_amountkrw)
                elif viz_choice == '2' and not df_4h_buysell_amountkrw.empty:
                    viz_results['timeline'] = visualizer.create_trading_timeline(df_4h_buysell_amountkrw)
                elif viz_choice == '3' and not df_4h_buysell_amountkrw.empty:
                    top_n = int(input("표시할 상위 사용자 수 (기본값: 20): ") or "20")
                    viz_results['ranking'] = visualizer.create_user_ranking_chart(df_4h_buysell_amountkrw, top_n)
                elif viz_choice == '4' and not df_day_buysell_info.empty:
                    viz_results['daily_pattern'] = visualizer.create_daily_pattern_analysis(df_day_buysell_info)
                elif viz_choice == '5':
                    viz_results = visualizer.create_all_visualizations(
                        df_4h_buysell_amountkrw, 
                        df_day_buysell_info
                    )
                
                # 생성된 시각화 파일 경로 출력
                print("\n시각화 파일 생성 완료:")
                for viz_type, path in viz_results.items():
                    if path:
                        print(f"  - {viz_type}: {path}")
            
            return result
        else:
            print("조회된 데이터가 없습니다.")
            return None
            
    except Exception as e:
        logger.error(f"통합 데이터 처리 중 오류 발생: {str(e)}")
        return None

def process_oracle_only():
    """Oracle 데이터만 처리"""
    try:
        processor = BlackMidProcessor()
        excel_processor = ExcelProcessor()
        
        xlsx_file_path = input("MID Excel 파일 경로를 입력하세요: ").strip()
        if not Path(xlsx_file_path).exists():
            logger.error(f"파일을 찾을 수 없습니다: {xlsx_file_path}")
            return None
        
        df = processor.process(xlsx_file_path, validate_format=True)
        
        if not df.empty:
            print(f"\n조회 완료: {len(df)}건")
            print(df.head())
            
            save_result = input("\n결과를 Excel로 저장하시겠습니까? (y/n): ").lower()
            if save_result == 'y':
                output_dir = Path('output')
                output_dir.mkdir(exist_ok=True)
                output_path = output_dir / f'oracle_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                excel_processor.save_dataframe(df, str(output_path))
                print(f"저장 완료: {output_path}")
            return df
        else:
            print("조회된 데이터가 없습니다.")
            return None
    except Exception as e:
        logger.error(f"Oracle 처리 중 오류: {str(e)}")
        return None

def run_custom_query():
    """사용자 정의 쿼리 실행"""
    try:
        oracle_config, redshift_config = load_config()
        query_loader = QueryLoader()
        excel_processor = ExcelProcessor()
        
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
        
        queries = query_loader.load_all_queries(db_type)
        if queries:
            print(f"\n사용 가능한 쿼리 파일:")
            for i, query_name in enumerate(queries.keys(), 1):
                print(f"{i}. {query_name}")
            
            query_idx = int(input("실행할 쿼리 번호를 선택하세요: ")) - 1
            query_name = list(queries.keys())[query_idx]
            
            with connector as db:
                query = queries[query_name]
                
                if ':' in query:
                    print("\n쿼리에 파라미터가 필요합니다.")
                    params = input("파라미터 값을 입력하세요 (쉼표로 구분): ").split(',')
                    df = db.execute_query(query, tuple(params))
                else:
                    df = db.execute_query(query)
                
                if not df.empty:
                    print(f"\n결과: {len(df)}개 행 조회됨")
                    print(df.head())
                    
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
        print("1. 통합 데이터 처리 (Oracle + Redshift + 거래 데이터)")
        print("2. Oracle 데이터만 처리")
        print("3. 사용자 정의 쿼리 실행")
        print("4. 종료")
        
        choice = input("\n선택: ").strip()
        
        if choice == '1':
            result = process_integrated_black_mid()
        elif choice == '2':
            df = process_oracle_only()
        elif choice == '3':
            df = run_custom_query()
        elif choice == '4':
            print("프로그램을 종료합니다.")
            break
        else:
            print("잘못된 선택입니다. 다시 시도해주세요.")
    
    logger.info("프로그램 종료")

if __name__ == "__main__":
    main()