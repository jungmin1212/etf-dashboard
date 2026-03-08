import streamlit as st
import pandas as pd
from pathlib import Path

# 웹페이지 기본 설정
st.set_page_config(page_title="BSOL-IBIT ETF 퀀트 대시보드", layout="wide")

# 데이터 불러오기 함수 (캐싱 활성화)
@st.cache_data 
def load_data(file_name):
    file_path = Path(file_name)
    if file_path.exists():
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()

st.title("ETF 추적 대시보드")
st.markdown("비트와이즈와 블랙록의 추정 평단가와 자금 흐름을 추적합니다.")
st.info("🔄 데이터는 매일 한국 시간 오후 12시(정오) 즈음에 자동으로 최신화됩니다.")

# 탭 나누기
tab1, tab2 = st.tabs(["솔라나 (BSOL)", "비트코인 (IBIT)"])

with tab1:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    
    if not df_bsol.empty:
        latest = df_bsol.iloc[-1]
        latest_date_str = latest['date'].strftime('%m월 %d일')
        
        # 지표 레이아웃 (5칸 구성)
        col1, col2, col3, col4, col5 = st.columns(5)
        
        px = latest['implied_sol_px']
        avg_cost = latest['avg_buy_price_ex_staking']
        gap_pct = (px - avg_cost) / avg_cost * 100
        
        col1.metric("현재 추정 시장가", f"${px:,.2f}")
        col2.metric("기관 순수 평단가", f"${avg_cost:,.2f}")
        col3.metric("평단가 대비 괴리율", f"{gap_pct:.2f}%", f"{gap_pct:.2f}%") 
        col4.metric(f"{latest_date_str} 순매수", f"{latest['flow_sol_final']:,.2f} SOL")
        # 보유량 표시 (정수형으로 깔끔하게)
        col5.metric("현재 추정 보유량", f"{latest['total_sol_held']:,.0f} SOL")
        
        st.subheader("평단가 vs 현재가 추세")
        chart_data = df_bsol[['date', 'implied_sol_px', 'avg_buy_price_ex_staking']].set_index('date')
        chart_data.columns = ['시장가 (Market Price)', '기관 평단가 (Cost Basis)']
        # 현재가: 흰색, 평단가: 연보라색
        st.line_chart(chart_data, color=["#FFFFFF", "#B19CD9"])
        
        st.subheader("기관 자금 흐름 (Flow)")
        flow_data_bsol = df_bsol[['date', 'flow_sol_final']].copy()
        flow_data_bsol['color'] = flow_data_bsol['flow_sol_final'].apply(lambda x: '#2ecc71' if x >= 0 else '#e74c3c')
        st.bar_chart(flow_data_bsol, x='date', y='flow_sol_final', color='color')
    else:
        st.warning("BSOL 데이터 파일이 없습니다. 경로를 확인해 주세요.")

with tab2:
    st.header("IBIT (iShares Bitcoin Trust)")
    df_ibit = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    
    if not df_ibit.empty:
        latest = df_ibit.iloc[-1]
        latest_date_str = latest['date'].strftime('%m월 %d일')
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        px = latest['implied_btc_px']
        avg_cost = latest['avg_buy_price_ex_fee']
        gap_pct = (px - avg_cost) / avg_cost * 100
        
        col1.metric("현재 추정 시장가", f"${px:,.2f}")
        col2.metric("기관 순수 평단가", f"${avg_cost:,.2f}")
        col3.metric("평단가 대비 괴리율", f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
        col4.metric(f"{latest_date_str} 순매수", f"{latest['flow_btc_final']:,.2f} BTC")
        # 보유량 표시
        col5.metric("현재 추정 보유량", f"{latest['total_btc_held']:,.2f} BTC")
        
        st.subheader("평단가 vs 현재가 추세")
        chart_data = df_ibit[['date', 'implied_btc_px', 'avg_buy_price_ex_fee']].set_index('date')
        chart_data.columns = ['시장가 (Market Price)', '기관 평단가 (Cost Basis)']
        # 현재가: 흰색, 평단가: 주황색
        st.line_chart(chart_data, color=["#FFFFFF", "#FF8C00"])
        
        st.subheader("기관 자금 흐름 (Flow)")
        flow_data_ibit = df_ibit[['date', 'flow_btc_final']].copy()
        flow_data_ibit['color'] = flow_data_ibit['flow_btc_final'].apply(lambda x: '#2ecc71' if x >= 0 else '#e74c3c')
        st.bar_chart(flow_data_ibit, x='date', y='flow_btc_final', color='color')
    else:
        st.warning("IBIT 데이터 파일이 없습니다. 경로를 확인해 주세요.")
