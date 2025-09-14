import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class TradingVisualizer:
    """거래 데이터 시각화 클래스"""
    
    def __init__(self, output_dir: str = 'output/visualizations'):
        """
        Args:
            output_dir: 시각화 파일 저장 디렉토리
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def create_4h_heatmap(self, df_4h: pd.DataFrame, title: str = "4시간 단위 거래금액 히트맵") -> str:
        """
        4시간 단위 거래금액 히트맵 생성
        
        Args:
            df_4h: 4시간 단위 거래 데이터 (mid, time_slot, total_amount_krw 필수)
            title: 히트맵 제목
        
        Returns:
            저장된 HTML 파일 경로
        """
        try:
            if df_4h.empty:
                logger.warning("데이터가 비어있어 히트맵을 생성할 수 없습니다.")
                return ""
            
            # 데이터 피벗 (mid를 행으로, time_slot을 열로)
            pivot_data = df_4h.pivot_table(
                index='mid',
                columns='time_slot',
                values='total_amount_krw',
                fill_value=0
            )
            
            # 시간 슬롯을 문자열로 변환 (더 읽기 쉽게)
            if not pivot_data.empty:
                pivot_data.columns = pd.to_datetime(pivot_data.columns).strftime('%m/%d %H시')
            
            # 히트맵 생성
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale='RdYlBu_r',  # 빨강(높음) -> 노랑 -> 파랑(낮음)
                colorbar=dict(
                    title="거래금액<br>(원)",
                    titleside="right",
                    tickmode="linear",
                    tick0=0,
                    dtick=pivot_data.values.max() / 10 if pivot_data.values.max() > 0 else 1
                ),
                text=np.round(pivot_data.values / 1000000, 1),  # 백만원 단위로 표시
                texttemplate="%{text}M",
                textfont={"size": 10},
                hovertemplate="MID: %{y}<br>시간: %{x}<br>거래금액: ₩%{z:,.0f}<extra></extra>"
            ))
            
            # 레이아웃 설정
            fig.update_layout(
                title={
                    'text': title,
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                xaxis={
                    'title': '시간 (4시간 단위)',
                    'tickangle': -45,
                    'side': 'bottom'
                },
                yaxis={
                    'title': 'MID (계정)',
                    'autorange': 'reversed'  # 위에서 아래로 정렬
                },
                height=max(400, len(pivot_data.index) * 20 + 200),  # MID 수에 따라 높이 조절
                width=max(800, len(pivot_data.columns) * 40 + 200),  # 시간 슬롯 수에 따라 너비 조절
                margin=dict(l=150, r=100, t=100, b=100),
                font=dict(size=12)
            )
            
            # HTML 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"heatmap_4h_trading_{timestamp}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            logger.info(f"히트맵 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"히트맵 생성 실패: {str(e)}")
            return ""
    
    def create_trading_timeline(self, df_4h: pd.DataFrame, title: str = "시간대별 거래 추이") -> str:
        """
        시간대별 거래 추이 라인 차트 생성
        
        Args:
            df_4h: 4시간 단위 거래 데이터
            title: 차트 제목
        
        Returns:
            저장된 HTML 파일 경로
        """
        try:
            if df_4h.empty:
                logger.warning("데이터가 비어있어 타임라인을 생성할 수 없습니다.")
                return ""
            
            # 시간별 전체 거래금액 집계
            timeline_data = df_4h.groupby('time_slot').agg({
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum',
                'total_amount_krw': 'sum',
                'mid': 'count'  # 활성 사용자 수
            }).reset_index()
            
            timeline_data.columns = ['time_slot', 'buy_total', 'sell_total', 'total', 'active_users']
            
            # 서브플롯 생성
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('거래금액 추이', '활성 사용자 수'),
                vertical_spacing=0.1,
                specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
            )
            
            # 거래금액 라인 차트
            fig.add_trace(
                go.Scatter(
                    x=timeline_data['time_slot'],
                    y=timeline_data['buy_total'],
                    mode='lines+markers',
                    name='매수',
                    line=dict(color='red', width=2),
                    marker=dict(size=6)
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=timeline_data['time_slot'],
                    y=timeline_data['sell_total'],
                    mode='lines+markers',
                    name='매도',
                    line=dict(color='blue', width=2),
                    marker=dict(size=6)
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=timeline_data['time_slot'],
                    y=timeline_data['total'],
                    mode='lines+markers',
                    name='전체',
                    line=dict(color='green', width=3, dash='dash'),
                    marker=dict(size=8)
                ),
                row=1, col=1
            )
            
            # 활성 사용자 수 바 차트
            fig.add_trace(
                go.Bar(
                    x=timeline_data['time_slot'],
                    y=timeline_data['active_users'],
                    name='활성 사용자',
                    marker_color='lightblue'
                ),
                row=2, col=1
            )
            
            # 레이아웃 업데이트
            fig.update_xaxes(title_text="시간", row=2, col=1, tickangle=-45)
            fig.update_yaxes(title_text="거래금액 (원)", row=1, col=1)
            fig.update_yaxes(title_text="사용자 수", row=2, col=1)
            
            fig.update_layout(
                title={
                    'text': title,
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                height=800,
                showlegend=True,
                hovermode='x unified'
            )
            
            # HTML 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"timeline_4h_trading_{timestamp}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            logger.info(f"타임라인 차트 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"타임라인 생성 실패: {str(e)}")
            return ""
    
    def create_user_ranking_chart(self, df_4h: pd.DataFrame, top_n: int = 20, 
                                 title: str = "상위 거래자 순위") -> str:
        """
        거래금액 기준 상위 사용자 바 차트 생성
        
        Args:
            df_4h: 4시간 단위 거래 데이터
            top_n: 표시할 상위 사용자 수
            title: 차트 제목
        
        Returns:
            저장된 HTML 파일 경로
        """
        try:
            if df_4h.empty:
                logger.warning("데이터가 비어있어 순위 차트를 생성할 수 없습니다.")
                return ""
            
            # 사용자별 거래금액 집계
            user_summary = df_4h.groupby('mid').agg({
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum',
                'total_amount_krw': 'sum',
                'trade_count': 'sum'
            }).reset_index()
            
            # 상위 N명 선택
            top_users = user_summary.nlargest(top_n, 'total_amount_krw')
            
            # 스택 바 차트 생성
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='매수',
                x=top_users['mid'],
                y=top_users['buy_amount_krw'],
                marker_color='red',
                text=top_users['buy_amount_krw'].apply(lambda x: f'₩{x/1e9:.1f}B'),
                textposition='inside'
            ))
            
            fig.add_trace(go.Bar(
                name='매도',
                x=top_users['mid'],
                y=top_users['sell_amount_krw'],
                marker_color='blue',
                text=top_users['sell_amount_krw'].apply(lambda x: f'₩{x/1e9:.1f}B'),
                textposition='inside'
            ))
            
            # 레이아웃 설정
            fig.update_layout(
                title={
                    'text': f"{title} (Top {top_n})",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                barmode='stack',
                xaxis={'title': 'MID', 'tickangle': -45},
                yaxis={'title': '거래금액 (원)'},
                height=600,
                showlegend=True,
                hovermode='x'
            )
            
            # HTML 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ranking_top{top_n}_{timestamp}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            logger.info(f"순위 차트 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"순위 차트 생성 실패: {str(e)}")
            return ""
    
    def create_daily_pattern_analysis(self, df_day: pd.DataFrame, title: str = "일별 거래 패턴 분석") -> str:
        """
        일별 거래 패턴 분석 대시보드 생성
        
        Args:
            df_day: 일별 거래 상세 데이터
            title: 대시보드 제목
        
        Returns:
            저장된 HTML 파일 경로
        """
        try:
            if df_day.empty:
                logger.warning("데이터가 비어있어 일별 패턴 분석을 생성할 수 없습니다.")
                return ""
            
            # 서브플롯 생성 (2x2 그리드)
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    '일별 거래금액 추이',
                    '마켓별 거래 비중',
                    '인기 종목 Top 10',
                    '일별 활성 사용자 및 거래 건수'
                ),
                specs=[
                    [{"type": "scatter"}, {"type": "pie"}],
                    [{"type": "bar"}, {"secondary_y": True}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.15
            )
            
            # 1. 일별 거래금액 추이
            daily_summary = df_day.groupby('trade_date').agg({
                'total_amount_krw': 'sum'
            }).reset_index()
            
            fig.add_trace(
                go.Scatter(
                    x=daily_summary['trade_date'],
                    y=daily_summary['total_amount_krw'],
                    mode='lines+markers',
                    name='일별 거래금액',
                    line=dict(color='green', width=2)
                ),
                row=1, col=1
            )
            
            # 2. 마켓별 거래 비중
            market_summary = df_day.groupby('market_nm')['total_amount_krw'].sum().reset_index()
            
            fig.add_trace(
                go.Pie(
                    labels=market_summary['market_nm'],
                    values=market_summary['total_amount_krw'],
                    name='마켓 비중'
                ),
                row=1, col=2
            )
            
            # 3. 인기 종목 Top 10
            ticker_summary = df_day.groupby('ticker_nm')['total_amount_krw'].sum().nlargest(10).reset_index()
            
            fig.add_trace(
                go.Bar(
                    x=ticker_summary['ticker_nm'],
                    y=ticker_summary['total_amount_krw'],
                    name='거래금액',
                    marker_color='lightblue'
                ),
                row=2, col=1
            )
            
            # 4. 일별 활성 사용자 및 거래 건수
            daily_activity = df_day.groupby('trade_date').agg({
                'mid': 'nunique',
                'total_trades': 'sum'
            }).reset_index()
            
            fig.add_trace(
                go.Bar(
                    x=daily_activity['trade_date'],
                    y=daily_activity['mid'],
                    name='활성 사용자',
                    marker_color='orange',
                    yaxis='y3'
                ),
                row=2, col=2
            )
            
            fig.add_trace(
                go.Scatter(
                    x=daily_activity['trade_date'],
                    y=daily_activity['total_trades'],
                    mode='lines+markers',
                    name='거래 건수',
                    line=dict(color='red', width=2),
                    yaxis='y4'
                ),
                row=2, col=2,
                secondary_y=True
            )
            
            # 레이아웃 업데이트
            fig.update_xaxes(title_text="날짜", row=1, col=1, tickangle=-45)
            fig.update_xaxes(title_text="종목", row=2, col=1, tickangle=-45)
            fig.update_xaxes(title_text="날짜", row=2, col=2, tickangle=-45)
            
            fig.update_yaxes(title_text="거래금액 (원)", row=1, col=1)
            fig.update_yaxes(title_text="거래금액 (원)", row=2, col=1)
            fig.update_yaxes(title_text="사용자 수", row=2, col=2)
            fig.update_yaxes(title_text="거래 건수", row=2, col=2, secondary_y=True)
            
            fig.update_layout(
                title={
                    'text': title,
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 24}
                },
                height=900,
                showlegend=True
            )
            
            # HTML 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"daily_pattern_analysis_{timestamp}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            logger.info(f"일별 패턴 분석 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"일별 패턴 분석 생성 실패: {str(e)}")
            return ""
    
    def create_all_visualizations(self, df_4h: pd.DataFrame, df_day: pd.DataFrame) -> dict:
        """
        모든 시각화를 한번에 생성
        
        Args:
            df_4h: 4시간 단위 거래 데이터
            df_day: 일별 거래 상세 데이터
        
        Returns:
            생성된 파일 경로 딕셔너리
        """
        results = {}
        
        logger.info("시각화 생성 시작...")
        
        # 1. 히트맵
        results['heatmap'] = self.create_4h_heatmap(df_4h)
        
        # 2. 타임라인
        results['timeline'] = self.create_trading_timeline(df_4h)
        
        # 3. 순위 차트
        results['ranking'] = self.create_user_ranking_chart(df_4h)
        
        # 4. 일별 패턴 분석
        if not df_day.empty:
            results['daily_pattern'] = self.create_daily_pattern_analysis(df_day)
        
        logger.info("모든 시각화 생성 완료")
        
        return results