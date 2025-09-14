import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
import json

logger = logging.getLogger(__name__)

class TradingVisualizer:
    """거래 데이터 시각화 클래스"""
    
    def __init__(self, output_dir: str = 'output/visualizations', 
                 plotly_mode: str = 'inline',
                 max_heatmap_rows: int = 100):
        """
        Args:
            output_dir: 시각화 파일 저장 디렉토리
            plotly_mode: Plotly 포함 방식 ('inline', 'cdn', 'directory', 'offline')
            max_heatmap_rows: 히트맵 최대 행 수 (성능 최적화)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.plotly_mode = plotly_mode
        self.max_heatmap_rows = max_heatmap_rows
        
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
    
    def get_plotly_include_mode(self):
        """Plotly 포함 모드 결정"""
        mode_mapping = {
            'inline': 'inline',      # HTML에 직접 포함 (가장 안전, 파일 크기 큼)
            'cdn': 'cdn',           # CDN 링크 (인터넷 필요)
            'directory': 'directory', # 별도 파일로 저장
            'offline': True,         # 오프라인 모드 (inline과 유사)
            'auto': 'auto'          # 자동 선택
        }
        return mode_mapping.get(self.plotly_mode, 'inline')
    
    def save_figure(self, fig, filename_prefix: str, use_compression: bool = False) -> str:
        """
        Figure를 HTML로 저장
        
        Args:
            fig: Plotly Figure 객체
            filename_prefix: 파일명 접두사
            use_compression: HTML 압축 여부
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}.html"
            filepath = self.output_dir / filename
            
            # Plotly 포함 모드 결정
            include_mode = self.get_plotly_include_mode()
            
            # HTML 저장 옵션
            write_options = {
                'config': {
                    'displayModeBar': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['pan2d', 'lasso2d'],
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': filename_prefix,
                        'height': 1080,
                        'width': 1920,
                        'scale': 1
                    }
                },
                'include_plotlyjs': include_mode,
                'div_id': f"plotly_div_{timestamp}"
            }
            
            # 압축 옵션 (실험적)
            if use_compression:
                write_options['auto_open'] = False
            
            # HTML 파일 생성
            fig.write_html(str(filepath), **write_options)
            
            # 파일 크기 확인 및 경고
            file_size_mb = filepath.stat().st_size / (1024 * 1024)
            if file_size_mb > 50:
                logger.warning(f"생성된 HTML 파일이 큽니다 ({file_size_mb:.1f}MB). 'cdn' 모드 사용을 고려하세요.")
            
            logger.info(f"시각화 저장 완료: {filepath} ({file_size_mb:.1f}MB)")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"시각화 저장 실패: {str(e)}")
            return ""
    
    def optimize_data_for_heatmap(self, pivot_data: pd.DataFrame, max_rows: int = None) -> pd.DataFrame:
        """
        히트맵용 데이터 최적화
        
        Args:
            pivot_data: 피벗 테이블 데이터
            max_rows: 최대 행 수
        """
        max_rows = max_rows or self.max_heatmap_rows
        
        if len(pivot_data.index) > max_rows:
            logger.info(f"히트맵 데이터 최적화: {len(pivot_data.index)}행 → {max_rows}행")
            # 거래액 합계 기준 상위 N개만 선택
            top_indices = pivot_data.sum(axis=1).nlargest(max_rows).index
            return pivot_data.loc[top_indices]
        return pivot_data
    
    def create_heatmap_1h(self, df_1h: pd.DataFrame, optimize: bool = True) -> str:
        """
        1시간 단위 거래 히트맵 생성
        
        Args:
            df_1h: 1시간 단위 거래 데이터
            optimize: 데이터 최적화 여부
        """
        try:
            if df_1h.empty:
                logger.warning("1시간 단위 데이터가 비어있습니다.")
                return ""
            
            # 피벗 테이블 생성
            pivot_data = df_1h.pivot_table(
                index='mid',
                columns='time_slot',
                values='total_amount_krw',
                fill_value=0
            )
            
            if pivot_data.empty:
                return ""
            
            # 데이터 최적화
            if optimize:
                pivot_data = self.optimize_data_for_heatmap(pivot_data)
            
            # 컬럼명 포맷팅
            pivot_data.columns = pd.to_datetime(pivot_data.columns).strftime('%m/%d %H시')
            
            # Figure 생성
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=self.theme['heat_colorscale'],
                colorbar=dict(
                    title="거래금액(원)",
                    tickfont=dict(color=self.theme['text_color'])
                ),
                hovertemplate="MID: %{y}<br>시간: %{x}<br>금액: ₩%{z:,.0f}<extra></extra>",
                zmin=0,  # 최소값 고정
                zmid=pivot_data.values.mean()  # 중간값 설정
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f"1시간 단위 거래 히트맵 (상위 {len(pivot_data)}개 MID)",
                height=max(600, min(len(pivot_data.index) * 20, 2000)),  # 최대 높이 제한
                **self.common_layout
            )
            
            # x축 라벨 조정
            if len(pivot_data.columns) > 50:
                fig.update_xaxes(tickmode='linear', dtick=2)
            
            return self.save_figure(fig, "heatmap_1h")
            
        except Exception as e:
            logger.error(f"1시간 히트맵 생성 실패: {str(e)}")
            return ""
    
    def create_heatmap_4h(self, df_4h: pd.DataFrame, optimize: bool = True) -> str:
        """4시간 단위 거래 히트맵 생성"""
        try:
            if df_4h.empty:
                logger.warning("4시간 단위 데이터가 비어있습니다.")
                return ""
            
            # 피벗 테이블 생성
            pivot_data = df_4h.pivot_table(
                index='mid',
                columns='time_slot',
                values='total_amount_krw',
                fill_value=0
            )
            
            if pivot_data.empty:
                return ""
            
            # 데이터 최적화
            if optimize:
                pivot_data = self.optimize_data_for_heatmap(pivot_data)
            
            # 컬럼명 포맷팅
            pivot_data.columns = pd.to_datetime(pivot_data.columns).strftime('%m/%d %H시')
            
            # Figure 생성
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=self.theme['heat_colorscale'],
                colorbar=dict(
                    title="거래금액(원)",
                    tickfont=dict(color=self.theme['text_color'])
                ),
                hovertemplate="MID: %{y}<br>시간: %{x}<br>금액: ₩%{z:,.0f}<extra></extra>",
                zmin=0,
                zmid=pivot_data.values.mean()
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f"4시간 단위 거래 히트맵 (상위 {len(pivot_data)}개 MID)",
                height=max(600, min(len(pivot_data.index) * 20, 2000)),
                **self.common_layout
            )
            
            return self.save_figure(fig, "heatmap_4h")
            
        except Exception as e:
            logger.error(f"4시간 히트맵 생성 실패: {str(e)}")
            return ""
    
    def create_heatmap_daily(self, df: pd.DataFrame, optimize: bool = True) -> str:
        """일별 거래 히트맵 생성"""
        try:
            if df.empty:
                logger.warning("데이터가 비어있습니다.")
                return ""
            
            # 일별 데이터 생성
            df_daily = df.copy()
            df_daily['date'] = pd.to_datetime(df_daily['time_slot']).dt.date
            
            # 일별 집계
            pivot_data = df_daily.groupby(['mid', 'date'])['total_amount_krw'].sum().reset_index()
            pivot_data = pivot_data.pivot_table(
                index='mid',
                columns='date',
                values='total_amount_krw',
                fill_value=0
            )
            
            if pivot_data.empty:
                return ""
            
            # 데이터 최적화
            if optimize:
                pivot_data = self.optimize_data_for_heatmap(pivot_data)
            
            # 컬럼명 포맷팅
            pivot_data.columns = pd.to_datetime(pivot_data.columns).strftime('%m/%d')
            
            # Figure 생성
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=self.theme['heat_colorscale'],
                colorbar=dict(
                    title="거래금액(원)",
                    tickfont=dict(color=self.theme['text_color'])
                ),
                hovertemplate="MID: %{y}<br>날짜: %{x}<br>금액: ₩%{z:,.0f}<extra></extra>",
                zmin=0,
                zmid=pivot_data.values.mean()
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f"일별 거래 히트맵 (상위 {len(pivot_data)}개 MID)",
                height=max(600, min(len(pivot_data.index) * 20, 2000)),
                **self.common_layout
            )
            
            return self.save_figure(fig, "heatmap_daily")
            
        except Exception as e:
            logger.error(f"일별 히트맵 생성 실패: {str(e)}")
            return ""
    
    def create_timeline_chart(self, df: pd.DataFrame) -> str:
        """시간대별 거래 추이 차트 생성"""
        try:
            if df.empty:
                logger.warning("데이터가 비어있습니다.")
                return ""
            
            # 시간대별 집계
            timeline_data = df.groupby('time_slot').agg({
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum',
                'total_amount_krw': 'sum'
            }).reset_index()
            
            # Figure 생성
            fig = go.Figure()
            
            # 매수 라인
            fig.add_trace(go.Scatter(
                x=timeline_data['time_slot'],
                y=timeline_data['buy_amount_krw'],
                mode='lines+markers',
                name='매수',
                line=dict(color=self.theme['primary_colors'][0], width=2),
                marker=dict(size=6),
                hovertemplate="시간: %{x}<br>매수액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 매도 라인
            fig.add_trace(go.Scatter(
                x=timeline_data['time_slot'],
                y=timeline_data['sell_amount_krw'],
                mode='lines+markers',
                name='매도',
                line=dict(color=self.theme['primary_colors'][2], width=2),
                marker=dict(size=6),
                hovertemplate="시간: %{x}<br>매도액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 총 거래액 라인
            fig.add_trace(go.Scatter(
                x=timeline_data['time_slot'],
                y=timeline_data['total_amount_krw'],
                mode='lines+markers',
                name='총 거래액',
                line=dict(color=self.theme['accent_colors'][0], width=3),
                marker=dict(size=8),
                hovertemplate="시간: %{x}<br>총액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title="시간대별 거래 추이",
                xaxis_title="시간",
                yaxis_title="거래금액 (원)",
                height=600,
                showlegend=True,
                legend=dict(
                    x=0.01,
                    y=0.99,
                    bgcolor='rgba(26, 30, 41, 0.8)'
                ),
                hovermode='x unified',
                **self.common_layout
            )
            
            return self.save_figure(fig, "timeline_chart")
            
        except Exception as e:
            logger.error(f"시간대별 추이 차트 생성 실패: {str(e)}")
            return ""
    
    def create_top_traders_chart(self, df: pd.DataFrame, top_n: int = 20) -> str:
        """상위 거래자 차트 생성"""
        try:
            if df.empty:
                logger.warning("데이터가 비어있습니다.")
                return ""
            
            # 사용자별 집계
            user_summary = df.groupby('mid').agg({
                'total_amount_krw': 'sum',
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum'
            }).reset_index().nlargest(top_n, 'total_amount_krw')
            
            # Figure 생성
            fig = go.Figure()
            
            # 매수 막대
            fig.add_trace(go.Bar(
                x=user_summary['mid'],
                y=user_summary['buy_amount_krw'],
                name='매수',
                marker_color=self.theme['primary_colors'][0],
                hovertemplate="MID: %{x}<br>매수액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 매도 막대
            fig.add_trace(go.Bar(
                x=user_summary['mid'],
                y=user_summary['sell_amount_krw'],
                name='매도',
                marker_color=self.theme['primary_colors'][2],
                hovertemplate="MID: %{x}<br>매도액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f"상위 거래자 Top {top_n}",
                xaxis_title="MID",
                yaxis_title="거래금액 (원)",
                height=600,
                barmode='stack',
                **self.common_layout
            )
            
            fig.update_xaxes(tickangle=-45)
            
            return self.save_figure(fig, f"top_{top_n}_traders")
            
        except Exception as e:
            logger.error(f"상위 거래자 차트 생성 실패: {str(e)}")
            return ""
    
    def create_market_pie_chart(self, df_day: pd.DataFrame) -> str:
        """마켓별 거래 비중 파이 차트 생성"""
        try:
            if df_day.empty or 'market_nm' not in df_day.columns:
                logger.warning("마켓 데이터가 없습니다.")
                return ""
            
            # 마켓별 집계
            market_summary = df_day.groupby('market_nm')['total_amount_krw'].sum().reset_index()
            market_summary = market_summary.sort_values('total_amount_krw', ascending=False)
            
            # Figure 생성
            fig = go.Figure(data=go.Pie(
                labels=market_summary['market_nm'],
                values=market_summary['total_amount_krw'],
                hole=0.4,
                marker=dict(colors=self.theme['primary_colors']),
                textfont=dict(color=self.theme['text_color'], size=12),
                textposition='auto',
                texttemplate='%{label}<br>%{percent}',
                hovertemplate="%{label}<br>₩%{value:,.0f}<br>%{percent}<extra></extra>"
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title="마켓별 거래 비중",
                height=600,
                showlegend=True,
                legend=dict(
                    font=dict(color=self.theme['text_color']),
                    bgcolor='rgba(26, 30, 41, 0.8)'
                ),
                **self.common_layout
            )
            
            return self.save_figure(fig, "market_pie")
            
        except Exception as e:
            logger.error(f"마켓별 비중 차트 생성 실패: {str(e)}")
            return ""
    
    def create_daily_pattern_chart(self, df_day: pd.DataFrame) -> str:
        """일별 거래 패턴 차트 생성"""
        try:
            if df_day.empty:
                logger.warning("일별 데이터가 비어있습니다.")
                return ""
            
            # 일별 집계
            daily_summary = df_day.groupby('trade_date').agg({
                'total_amount_krw': 'sum',
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum',
                'mid': 'nunique'
            }).reset_index()
            
            # Figure 생성 (서브플롯)
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('일별 거래금액', '일별 활성 사용자'),
                vertical_spacing=0.15,
                row_heights=[0.6, 0.4]
            )
            
            # 거래금액 차트
            fig.add_trace(
                go.Scatter(
                    x=daily_summary['trade_date'],
                    y=daily_summary['total_amount_krw'],
                    mode='lines+markers',
                    name='총 거래액',
                    line=dict(color=self.theme['accent_colors'][0], width=3),
                    marker=dict(size=8),
                    hovertemplate="날짜: %{x}<br>총액: ₩%{y:,.0f}<extra></extra>"
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=daily_summary['trade_date'],
                    y=daily_summary['buy_amount_krw'],
                    mode='lines',
                    name='매수',
                    line=dict(color=self.theme['primary_colors'][0], width=2),
                    hovertemplate="날짜: %{x}<br>매수액: ₩%{y:,.0f}<extra></extra>"
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=daily_summary['trade_date'],
                    y=daily_summary['sell_amount_krw'],
                    mode='lines',
                    name='매도',
                    line=dict(color=self.theme['primary_colors'][2], width=2),
                    hovertemplate="날짜: %{x}<br>매도액: ₩%{y:,.0f}<extra></extra>"
                ),
                row=1, col=1
            )
            
            # 활성 사용자 차트
            fig.add_trace(
                go.Bar(
                    x=daily_summary['trade_date'],
                    y=daily_summary['mid'],
                    name='활성 사용자',
                    marker_color=self.theme['primary_colors'][4],
                    hovertemplate="날짜: %{x}<br>사용자: %{y}명<extra></extra>"
                ),
                row=2, col=1
            )
            
            # 레이아웃 업데이트
            fig.update_layout(
                title="일별 거래 패턴",
                height=800,
                showlegend=True,
                legend=dict(
                    x=0.01,
                    y=0.99,
                    bgcolor='rgba(26, 30, 41, 0.8)'
                ),
                plot_bgcolor=self.theme['bg_color'],
                paper_bgcolor=self.theme['paper_color'],
                font={'color': self.theme['text_color']},
                hovermode='x unified'
            )
            
            # 축 업데이트
            fig.update_xaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            fig.update_yaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            
            return self.save_figure(fig, "daily_pattern")
            
        except Exception as e:
            logger.error(f"일별 패턴 차트 생성 실패: {str(e)}")
            return ""
    
    def create_ticker_volume_chart(self, df_day: pd.DataFrame, top_n: int = 10) -> str:
        """종목별 거래량 차트 생성"""
        try:
            if df_day.empty or 'ticker_nm' not in df_day.columns:
                logger.warning("종목 데이터가 없습니다.")
                return ""
            
            # 종목별 집계
            ticker_summary = df_day.groupby('ticker_nm').agg({
                'total_amount_krw': 'sum',
                'buy_amount_krw': 'sum',
                'sell_amount_krw': 'sum'
            }).reset_index()
            ticker_summary = ticker_summary.nlargest(top_n, 'total_amount_krw')
            
            # Figure 생성
            fig = go.Figure()
            
            # 매수 막대
            fig.add_trace(go.Bar(
                x=ticker_summary['ticker_nm'],
                y=ticker_summary['buy_amount_krw'],
                name='매수',
                marker_color=self.theme['primary_colors'][0],
                hovertemplate="종목: %{x}<br>매수액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 매도 막대
            fig.add_trace(go.Bar(
                x=ticker_summary['ticker_nm'],
                y=ticker_summary['sell_amount_krw'],
                name='매도',
                marker_color=self.theme['primary_colors'][2],
                hovertemplate="종목: %{x}<br>매도액: ₩%{y:,.0f}<extra></extra>"
            ))
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f"종목별 거래량 Top {top_n}",
                xaxis_title="종목",
                yaxis_title="거래금액 (원)",
                height=600,
                barmode='stack',
                **self.common_layout
            )
            
            return self.save_figure(fig, f"ticker_volume_top_{top_n}")
            
        except Exception as e:
            logger.error(f"종목별 거래량 차트 생성 실패: {str(e)}")
            return ""
    
    def create_active_users_chart(self, df_day: pd.DataFrame) -> str:
        """활성 사용자 추이 차트 생성"""
        try:
            if df_day.empty:
                logger.warning("데이터가 비어있습니다.")
                return ""
            
            # 일별 활성 사용자 집계
            daily_users = df_day.groupby('trade_date').agg({
                'mid': 'nunique',
                'total_amount_krw': 'sum'
            }).reset_index()
            daily_users.columns = ['trade_date', 'active_users', 'total_amount']
            
            # 사용자당 평균 거래액 계산
            daily_users['avg_amount_per_user'] = daily_users['total_amount'] / daily_users['active_users']
            
            # Figure 생성 (서브플롯)
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('활성 사용자 수', '사용자당 평균 거래액'),
                vertical_spacing=0.15,
                specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
            )
            
            # 활성 사용자 수
            fig.add_trace(
                go.Scatter(
                    x=daily_users['trade_date'],
                    y=daily_users['active_users'],
                    mode='lines+markers',
                    name='활성 사용자',
                    line=dict(color=self.theme['primary_colors'][4], width=3),
                    marker=dict(size=8),
                    fill='tozeroy',
                    fillcolor='rgba(102, 153, 255, 0.2)',
                    hovertemplate="날짜: %{x}<br>사용자: %{y}명<extra></extra>"
                ),
                row=1, col=1
            )
            
            # 사용자당 평균 거래액
            fig.add_trace(
                go.Bar(
                    x=daily_users['trade_date'],
                    y=daily_users['avg_amount_per_user'],
                    name='평균 거래액',
                    marker_color=self.theme['accent_colors'][0],
                    hovertemplate="날짜: %{x}<br>평균: ₩%{y:,.0f}<extra></extra>"
                ),
                row=2, col=1
            )
            
            # 레이아웃 업데이트
            fig.update_layout(
                title="활성 사용자 분석",
                height=800,
                showlegend=True,
                legend=dict(
                    x=0.01,
                    y=0.99,
                    bgcolor='rgba(26, 30, 41, 0.8)'
                ),
                plot_bgcolor=self.theme['bg_color'],
                paper_bgcolor=self.theme['paper_color'],
                font={'color': self.theme['text_color']},
                hovermode='x unified'
            )
            
            # 축 업데이트
            fig.update_xaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            fig.update_yaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            fig.update_yaxes(title_text="활성 사용자 수", row=1, col=1)
            fig.update_yaxes(title_text="평균 거래액 (원)", row=2, col=1)
            
            return self.save_figure(fig, "active_users")
            
        except Exception as e:
            logger.error(f"활성 사용자 차트 생성 실패: {str(e)}")
            return ""
    
    def create_integrated_dashboard(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame, 
                                  df_day: pd.DataFrame) -> str:
        """
        통합 대시보드 생성 (경량화 버전)
        대용량 데이터의 경우 샘플링하여 표시
        """
        try:
            logger.info("통합 대시보드 생성 시작 (경량화 모드)")
            
            # 데이터 크기 확인 및 샘플링
            total_rows = len(df_1h) + len(df_4h) + len(df_day)
            
            if total_rows > 10000:
                logger.warning(f"대용량 데이터 감지 ({total_rows}행). 샘플링 적용.")
                
                # 상위 50개 MID만 선택
                if not df_4h.empty:
                    top_mids = df_4h.groupby('mid')['total_amount_krw'].sum().nlargest(50).index
                    df_1h_sample = df_1h[df_1h['mid'].isin(top_mids)] if not df_1h.empty else df_1h
                    df_4h_sample = df_4h[df_4h['mid'].isin(top_mids)]
                    df_day_sample = df_day[df_day['mid'].isin(top_mids)] if not df_day.empty else df_day
                else:
                    df_1h_sample = df_1h
                    df_4h_sample = df_4h
                    df_day_sample = df_day
            else:
                df_1h_sample = df_1h
                df_4h_sample = df_4h
                df_day_sample = df_day
            
            # 간단한 대시보드 생성 (주요 지표만)
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    '시간대별 거래 추이',
                    '상위 거래자 Top 10',
                    '마켓별 거래 비중',
                    '일별 거래 패턴'
                ),
                specs=[
                    [{"type": "scatter"}, {"type": "bar"}],
                    [{"type": "pie"}, {"type": "scatter"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.15
            )
            
            # 1. 시간대별 거래 추이
            df_for_timeline = df_4h_sample if not df_4h_sample.empty else df_1h_sample
            if not df_for_timeline.empty:
                timeline_data = df_for_timeline.groupby('time_slot')['total_amount_krw'].sum().reset_index()
                fig.add_trace(
                    go.Scatter(
                        x=timeline_data['time_slot'],
                        y=timeline_data['total_amount_krw'],
                        mode='lines',
                        name='거래액',
                        line=dict(color=self.theme['primary_colors'][0])
                    ),
                    row=1, col=1
                )
            
            # 2. 상위 거래자
            if not df_for_timeline.empty:
                user_summary = df_for_timeline.groupby('mid')['total_amount_krw'].sum().nlargest(10).reset_index()
                fig.add_trace(
                    go.Bar(
                        x=user_summary['mid'],
                        y=user_summary['total_amount_krw'],
                        marker_color=self.theme['primary_colors'][1],
                        showlegend=False
                    ),
                    row=1, col=2
                )
            
            # 3. 마켓별 비중
            if not df_day_sample.empty and 'market_nm' in df_day_sample.columns:
                market_summary = df_day_sample.groupby('market_nm')['total_amount_krw'].sum().reset_index()
                fig.add_trace(
                    go.Pie(
                        labels=market_summary['market_nm'],
                        values=market_summary['total_amount_krw'],
                        hole=0.4,
                        marker=dict(colors=self.theme['primary_colors']),
                        showlegend=True
                    ),
                    row=2, col=1
                )
            
            # 4. 일별 패턴
            if not df_day_sample.empty:
                daily_summary = df_day_sample.groupby('trade_date')['total_amount_krw'].sum().reset_index()
                fig.add_trace(
                    go.Scatter(
                        x=daily_summary['trade_date'],
                        y=daily_summary['total_amount_krw'],
                        mode='lines+markers',
                        name='일별 거래액',
                        line=dict(color=self.theme['accent_colors'][0])
                    ),
                    row=2, col=2
                )
            
            # 레이아웃 업데이트
            fig.update_layout(
                title="Black Heatmap 거래 대시보드 (경량화)",
                height=800,
                showlegend=False,
                plot_bgcolor=self.theme['bg_color'],
                paper_bgcolor=self.theme['paper_color'],
                font={'color': self.theme['text_color']}
            )
            
            # 축 업데이트
            fig.update_xaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            fig.update_yaxes(gridcolor=self.theme['grid_color'], color=self.theme['text_color'])
            
            return self.save_figure(fig, "dashboard_lite")
            
        except Exception as e:
            logger.error(f"통합 대시보드 생성 실패: {str(e)}")
            return ""