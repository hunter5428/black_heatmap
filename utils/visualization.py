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
        
        # 다크 테마 색상 팔레트
        self.theme = {
            'bg_color': '#0e1117',  # 배경색 (다크)
            'paper_color': '#1a1e29',  # 서브 배경색
            'text_color': '#ffffff',  # 텍스트 색상
            'grid_color': '#2a3f4f',  # 그리드 색상
            'primary_colors': ['#00d4ff', '#0099ff', '#0066ff', '#3366ff', '#6699ff'],  # 블루 계열
            'accent_colors': ['#00ffcc', '#33ffcc', '#66ffcc'],  # 민트 계열
            'heat_colorscale': [  # 히트맵 색상
                [0, '#0e1117'],      # 최소값 - 배경색
                [0.2, '#1a3a52'],    # 진한 파랑
                [0.4, '#2e5c8a'],    # 파랑
                [0.6, '#4a8bc2'],    # 하늘색
                [0.8, '#73b9f5'],    # 밝은 하늘색
                [1, '#a6d8ff']       # 매우 밝은 하늘색
            ]
        }
        
        # 공통 레이아웃 설정
        self.common_layout = {
            'plot_bgcolor': self.theme['bg_color'],
            'paper_bgcolor': self.theme['paper_color'],
            'font': {
                'color': self.theme['text_color'],
                'family': 'Arial, sans-serif',
                'size': 12
            },
            'xaxis': {
                'gridcolor': self.theme['grid_color'],
                'zerolinecolor': self.theme['grid_color'],
                'color': self.theme['text_color']
            },
            'yaxis': {
                'gridcolor': self.theme['grid_color'],
                'zerolinecolor': self.theme['grid_color'],
                'color': self.theme['text_color']
            }
        }
    
    def prepare_heatmap_data(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> dict:
        """
        다양한 시간 단위로 히트맵 데이터 준비
        
        Args:
            df_1h: 1시간 단위 거래 데이터
            df_4h: 4시간 단위 거래 데이터
        
        Returns:
            시간 단위별 피벗 데이터
        """
        result = {}
        
        # 1시간 단위 데이터
        if not df_1h.empty:
            pivot_1h = df_1h.pivot_table(
                index='mid',
                columns='time_slot',
                values='total_amount_krw',
                fill_value=0
            )
            if not pivot_1h.empty:
                pivot_1h.columns = pd.to_datetime(pivot_1h.columns).strftime('%m/%d %H시')
            result['1h'] = pivot_1h
        
        # 4시간 단위 데이터
        if not df_4h.empty:
            pivot_4h = df_4h.pivot_table(
                index='mid',
                columns='time_slot',
                values='total_amount_krw',
                fill_value=0
            )
            if not pivot_4h.empty:
                pivot_4h.columns = pd.to_datetime(pivot_4h.columns).strftime('%m/%d %H시')
            result['4h'] = pivot_4h
        
        # 일 단위 데이터 생성 (1시간 또는 4시간 데이터에서)
        df_for_daily = df_1h if not df_1h.empty else df_4h
        if not df_for_daily.empty:
            df_daily = df_for_daily.copy()
            df_daily['date'] = pd.to_datetime(df_daily['time_slot']).dt.date
            pivot_daily = df_daily.groupby(['mid', 'date'])['total_amount_krw'].sum().reset_index()
            pivot_daily = pivot_daily.pivot_table(
                index='mid',
                columns='date',
                values='total_amount_krw',
                fill_value=0
            )
            if not pivot_daily.empty:
                pivot_daily.columns = pd.to_datetime(pivot_daily.columns).strftime('%m/%d')
            result['daily'] = pivot_daily
        
        return result
    
    def create_integrated_dashboard(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_day: pd.DataFrame) -> str:
        """
        통합 대시보드 생성 (히트맵을 상단 전체 너비로 배치)
        
        Args:
            df_1h: 1시간 단위 거래 데이터
            df_4h: 4시간 단위 거래 데이터
            df_day: 일별 거래 상세 데이터
        
        Returns:
            저장된 HTML 파일 경로
        """
        try:
            # 히트맵 데이터 준비
            heatmap_data = self.prepare_heatmap_data(df_1h, df_4h)
            
            # 서브플롯 생성 (4행 2열 - 히트맵은 상단 전체)
            fig = make_subplots(
                rows=4, cols=2,
                subplot_titles=(
                    '거래 히트맵 (시간대별 거래금액)', '',
                    '시간대별 거래 추이', '상위 거래자 Top 20',
                    '마켓별 거래 비중', '일별 거래 패턴',
                    '종목별 거래량 Top 10', ''
                ),
                specs=[
                    [{"colspan": 2, "type": "heatmap"}, None],  # 히트맵 - 전체 너비
                    [{"type": "scatter"}, {"type": "bar"}],      # 차트 2개
                    [{"type": "pie"}, {"type": "scatter"}],      # 차트 2개
                    [{"type": "bar"}, {"type": "scatter"}]       # 차트 2개 (빈 공간 활용)
                ],
                vertical_spacing=0.08,
                horizontal_spacing=0.12,
                row_heights=[0.35, 0.25, 0.25, 0.15]  # 히트맵을 크게
            )
            
            # 1. 히트맵 (상단 전체 너비)
            default_heatmap = '4h' if '4h' in heatmap_data else ('1h' if '1h' in heatmap_data else 'daily')
            if default_heatmap in heatmap_data and not heatmap_data[default_heatmap].empty:
                pivot_data = heatmap_data[default_heatmap]
                fig.add_trace(
                    go.Heatmap(
                        z=pivot_data.values,
                        x=pivot_data.columns,
                        y=pivot_data.index,
                        colorscale=self.theme['heat_colorscale'],
                        colorbar=dict(
                            title="거래금액(원)",
                            tickfont=dict(color=self.theme['text_color']),
                            len=0.3,
                            y=0.85,
                            yanchor="top"
                        ),
                        hovertemplate="MID: %{y}<br>시간: %{x}<br>금액: ₩%{z:,.0f}<extra></extra>",
                        showscale=True
                    ),
                    row=1, col=1
                )
            
            # 2. 시간대별 거래 추이
            df_for_timeline = df_4h if not df_4h.empty else df_1h
            if not df_for_timeline.empty:
                timeline_data = df_for_timeline.groupby('time_slot').agg({
                    'buy_amount_krw': 'sum',
                    'sell_amount_krw': 'sum',
                    'total_amount_krw': 'sum'
                }).reset_index()
                
                fig.add_trace(
                    go.Scatter(
                        x=timeline_data['time_slot'],
                        y=timeline_data['buy_amount_krw'],
                        mode='lines+markers',
                        name='매수',
                        line=dict(color=self.theme['primary_colors'][0], width=2),
                        marker=dict(size=6)
                    ),
                    row=2, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=timeline_data['time_slot'],
                        y=timeline_data['sell_amount_krw'],
                        mode='lines+markers',
                        name='매도',
                        line=dict(color=self.theme['primary_colors'][2], width=2),
                        marker=dict(size=6)
                    ),
                    row=2, col=1
                )
            
            # 3. 상위 거래자 Top 20
            if not df_for_timeline.empty:
                user_summary = df_for_timeline.groupby('mid').agg({
                    'total_amount_krw': 'sum'
                }).reset_index().nlargest(20, 'total_amount_krw')
                
                fig.add_trace(
                    go.Bar(
                        x=user_summary['mid'],
                        y=user_summary['total_amount_krw'],
                        marker_color=self.theme['primary_colors'][1],
                        text=user_summary['total_amount_krw'].apply(lambda x: f'₩{x/1e9:.1f}B'),
                        textposition='outside',
                        textfont=dict(color=self.theme['text_color'], size=10),
                        hovertemplate="MID: %{x}<br>금액: ₩%{y:,.0f}<extra></extra>",
                        showlegend=False
                    ),
                    row=2, col=2
                )
            
            # 4. 마켓별 거래 비중
            if not df_day.empty and 'market_nm' in df_day.columns:
                market_summary = df_day.groupby('market_nm')['total_amount_krw'].sum().reset_index()
                
                fig.add_trace(
                    go.Pie(
                        labels=market_summary['market_nm'],
                        values=market_summary['total_amount_krw'],
                        hole=0.4,
                        marker=dict(colors=self.theme['primary_colors']),
                        textfont=dict(color=self.theme['text_color']),
                        hovertemplate="%{label}<br>₩%{value:,.0f}<br>%{percent}<extra></extra>",
                        showlegend=True
                    ),
                    row=3, col=1
                )
            
            # 5. 일별 거래 패턴
            if not df_day.empty:
                daily_summary = df_day.groupby('trade_date').agg({
                    'total_amount_krw': 'sum',
                    'mid': 'nunique'
                }).reset_index()
                
                fig.add_trace(
                    go.Scatter(
                        x=daily_summary['trade_date'],
                        y=daily_summary['total_amount_krw'],
                        mode='lines+markers',
                        name='일별 거래액',
                        line=dict(color=self.theme['accent_colors'][0], width=3),
                        marker=dict(size=8),
                        hovertemplate="날짜: %{x}<br>금액: ₩%{y:,.0f}<extra></extra>"
                    ),
                    row=3, col=2
                )
            
            # 6. 종목별 거래량 Top 10
            if not df_day.empty and 'ticker_nm' in df_day.columns:
                ticker_summary = df_day.groupby('ticker_nm')['total_amount_krw'].sum().nlargest(10).reset_index()
                
                fig.add_trace(
                    go.Bar(
                        x=ticker_summary['ticker_nm'],
                        y=ticker_summary['total_amount_krw'],
                        marker_color=self.theme['primary_colors'][3],
                        text=ticker_summary['total_amount_krw'].apply(lambda x: f'₩{x/1e6:.0f}M'),
                        textposition='outside',
                        textfont=dict(color=self.theme['text_color'], size=10),
                        hovertemplate="종목: %{x}<br>금액: ₩%{y:,.0f}<extra></extra>",
                        showlegend=False
                    ),
                    row=4, col=1
                )
            
            # 7. 활성 사용자 추이 (추가)
            if not df_day.empty:
                daily_users = df_day.groupby('trade_date')['mid'].nunique().reset_index()
                daily_users.columns = ['trade_date', 'active_users']
                
                fig.add_trace(
                    go.Scatter(
                        x=daily_users['trade_date'],
                        y=daily_users['active_users'],
                        mode='lines+markers',
                        name='활성 사용자',
                        line=dict(color=self.theme['primary_colors'][4], width=2),
                        marker=dict(size=6),
                        hovertemplate="날짜: %{x}<br>사용자: %{y}명<extra></extra>"
                    ),
                    row=4, col=2
                )
            
            # 레이아웃 업데이트
            fig.update_layout(
                title={
                    'text': 'Black Heatmap Trading Dashboard',
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 28, 'color': self.theme['text_color']}
                },
                showlegend=True,
                legend=dict(
                    font=dict(color=self.theme['text_color']),
                    bgcolor='rgba(26, 30, 41, 0.8)',
                    x=0.01,
                    y=0.55
                ),
                height=1400,  # 높이 증가
                plot_bgcolor=self.theme['bg_color'],
                paper_bgcolor=self.theme['paper_color'],
                font={'color': self.theme['text_color']},
                hovermode='closest'
            )
            
            # 모든 축 스타일 업데이트
            fig.update_xaxes(
                gridcolor=self.theme['grid_color'],
                color=self.theme['text_color'],
                tickangle=-45
            )
            fig.update_yaxes(
                gridcolor=self.theme['grid_color'],
                color=self.theme['text_color']
            )
            
            # 히트맵 축 특별 처리 (더 많은 라벨 표시)
            fig.update_xaxes(
                row=1, col=1,
                tickmode='linear',
                tick0=0,
                dtick=1 if len(pivot_data.columns) < 50 else 2
            )
            
            # 드롭다운 메뉴 추가 (히트맵 시간 단위 선택)
            updatemenus = []
            if heatmap_data:
                buttons = []
                
                if '1h' in heatmap_data and not heatmap_data['1h'].empty:
                    buttons.append(
                        dict(
                            label="1시간",
                            method="restyle",
                            args=[{
                                "z": [heatmap_data['1h'].values],
                                "x": [heatmap_data['1h'].columns],
                                "y": [heatmap_data['1h'].index]
                            }, [0]]
                        )
                    )
                
                if '4h' in heatmap_data and not heatmap_data['4h'].empty:
                    buttons.append(
                        dict(
                            label="4시간",
                            method="restyle",
                            args=[{
                                "z": [heatmap_data['4h'].values],
                                "x": [heatmap_data['4h'].columns],
                                "y": [heatmap_data['4h'].index]
                            }, [0]]
                        )
                    )
                
                if 'daily' in heatmap_data and not heatmap_data['daily'].empty:
                    buttons.append(
                        dict(
                            label="1일",
                            method="restyle",
                            args=[{
                                "z": [heatmap_data['daily'].values],
                                "x": [heatmap_data['daily'].columns],
                                "y": [heatmap_data['daily'].index]
                            }, [0]]
                        )
                    )
                
                if buttons:
                    updatemenus.append(
                        dict(
                            type="dropdown",
                            buttons=buttons,
                            x=0.02,
                            y=0.95,
                            xanchor="left",
                            yanchor="top",
                            bgcolor='rgba(26, 30, 41, 0.9)',
                            bordercolor=self.theme['primary_colors'][0],
                            font=dict(color=self.theme['text_color'])
                        )
                    )
            
            if updatemenus:
                fig.update_layout(updatemenus=updatemenus)
            
            # HTML 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"trading_dashboard_{timestamp}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                config={
                    'displayModeBar': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['pan2d', 'lasso2d']
                },
                include_plotlyjs='cdn'
            )
            
            logger.info(f"통합 대시보드 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"통합 대시보드 생성 실패: {str(e)}")
            return ""
    
    def create_all_visualizations(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_day: pd.DataFrame) -> dict:
        """
        모든 시각화를 통합 대시보드로 생성
        
        Args:
            df_1h: 1시간 단위 거래 데이터
            df_4h: 4시간 단위 거래 데이터
            df_day: 일별 거래 상세 데이터
        
        Returns:
            생성된 파일 경로 딕셔너리
        """
        results = {}
        
        logger.info("통합 대시보드 생성 시작...")
        
        # 통합 대시보드 생성
        dashboard_path = self.create_integrated_dashboard(df_1h, df_4h, df_day)
        if dashboard_path:
            results['integrated_dashboard'] = dashboard_path
        
        logger.info("통합 대시보드 생성 완료")
        
        return results