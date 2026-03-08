import streamlit as st
import pandas as pd
from pathlib import Path

# 웹페이지 기본 설정 (제목, 레이아웃 넓게)
st.set_page_config(page_title="BSOL-IBIT ETF 퀀트 대시보드", layout="wide")

# 데이터 불러오기 함수 (캐싱을 통해 속도 향상)
@st.cache_data(ttl=3600) # 1시간마다 데이터 새로고침
def load_data(file_name):
    # 각 스크립트가 저장하는 폴더 경로에 맞게 설정
    # bsol은 bsol_tracker 폴더, ibit은 ibit_tracker 폴더 안에 있다고 가정합니다.
    file_path = Path(file_name)
    if file_path.exists():
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()

st.title("  ETF 추적 대시보드")
st.markdown("비트와이즈와 블랙록의 추정 평단가와 자금 흐름을 실시간으로 추적합니다." 업데이트 매일 오후 12시)

# 탭을 나누어 BSOL과 IBIT를 깔끔하게 분리
tab1, tab2 = st.tabs([" 솔라나 (BSOL)", " 비트코인 (IBIT)"])

with tab1:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    
    if not df_bsol.empty:
        latest = df_bsol.iloc[-1]
        
        # 최신 데이터 요약 지표 (Metrics)
        col1, col2, col3, col4 = st.columns(4)
        
        px = latest['implied_sol_px']
        avg_cost = latest['avg_buy_price_ex_staking']
        gap_pct = (px - avg_cost) / avg_cost * 100
        
        col1.metric("현재 추정 시장가", f"${px:,.2f}")
        col2.metric("기관 순수 평단가", f"${avg_cost:,.2f}")
        # 괴리율은 델타(변화량) 색상으로 표시
        col3.metric("평단가 대비 괴리율", f"{gap_pct:.2f}%", f"{gap_pct:.2f}%") 
        col4.metric("오늘 순매수(SOL)", f"{latest['flow_sol_final']:,.2f}")
        
        st.subheader(" 평단가 vs 현재가 추세")
        # 차트를 그리기 위해 필요한 열만 추출
        chart_data = df_bsol[['date', 'implied_sol_px', 'avg_buy_price_ex_staking']].set_index('date')
        chart_data.columns = ['시장가 (Market Price)', '기관 평단가 (Cost Basis)']
        st.line_chart(chart_data)
        
        st.subheader(" 기관 자금 흐름 (Flow)")
        flow_data = df_bsol[['date', 'flow_sol_final']].set_index('date')
        st.bar_chart(flow_data)
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

with tab2:
    st.header("IBIT (iShares Bitcoin Trust)")
    df_ibit = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    
    if not df_ibit.empty:
        latest = df_ibit.iloc[-1]
        
        col1, col2, col3, col4 = st.columns(4)
        
        px = latest['implied_btc_px']
        avg_cost = latest['avg_buy_price_ex_fee']
        gap_pct = (px - avg_cost) / avg_cost * 100
        
        col1.metric("현재 추정 시장가", f"${px:,.2f}")
        col2.metric("기관 순수 평단가", f"${avg_cost:,.2f}")
        col3.metric("평단가 대비 괴리율", f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
        col4.metric("오늘 순매수(BTC)", f"{latest['flow_btc_final']:,.2f}")
        
        st.subheader(" 평단가 vs 현재가 추세")
        chart_data = df_ibit[['date', 'implied_btc_px', 'avg_buy_price_ex_fee']].set_index('date')
        chart_data.columns = ['시장가 (Market Price)', '기관 평단가 (Cost Basis)']
        st.line_chart(chart_data)
        
        st.subheader(" 기관 자금 흐름 (Flow)")
        flow_data = df_ibit[['date', 'flow_btc_final']].set_index('date')
        st.bar_chart(flow_data)
    else:

        st.warning("IBIT 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")


