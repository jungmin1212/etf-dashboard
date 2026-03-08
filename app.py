import streamlit as st
import pandas as pd
import requests
from pathlib import Path

st.set_page_config(page_title="BSOL-IBIT ETF 퀀트 대시보드", layout="wide")

@st.cache_data(ttl=600)
def load_data(file_name: str) -> pd.DataFrame:
    file_path = Path(file_name)
    if file_path.exists():
        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame()

@st.cache_data(ttl=10)
def get_crypto_prices() -> dict:
    """CoinGecko API로 SOL/BTC 현재가 동시 조회
    (Binance는 Streamlit Cloud 서버 IP를 차단하므로 CoinGecko 사용)
    """
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana,bitcoin", "vs_currencies": "usd"},
            headers={"accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "SOL": float(data["solana"]["usd"]),
            "BTC": float(data["bitcoin"]["usd"]),
        }
    except Exception:
        return {"SOL": None, "BTC": None}


@st.fragment(run_every=10)
def bsol_live(df: pd.DataFrame) -> None:
    latest   = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_staking"])
    holdings = float(latest["sol_in_trust"])
    date_str = latest["date"].strftime("%m월 %d일")

    prices  = get_crypto_prices()
    px      = prices["SOL"] or float(latest["implied_sol_px"])
    gap_pct = (px - avg_cost) / avg_cost * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 시장가 (실시간)",      f"${px:,.2f}")
    c2.metric("기관 순수 평단가",          f"${avg_cost:,.2f}")
    c3.metric("평단가 대비 괴리율",        f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
    c4.metric(f"{date_str} 순매수(SOL)",  f"{latest['flow_sol_final']:,.2f}")
    c5.metric("추정 보유량 (SOL)",         f"{holdings:,.0f}")

    st.subheader("평단가 vs 현재가 추세")
    chart = df[["date", "implied_sol_px", "avg_buy_price_ex_staking"]].copy()
    chart.loc[chart.index[-1], "implied_sol_px"] = px
    chart = chart.set_index("date")
    chart.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart, color=["#FFFFFF", "#B19CD9"])


@st.fragment(run_every=10)
def ibit_live(df: pd.DataFrame) -> None:
    latest   = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_fee"])
    holdings = float(latest["btc_in_trust"])
    date_str = latest["date"].strftime("%m월 %d일")

    prices  = get_crypto_prices()
    px      = prices["BTC"] or float(latest["implied_btc_px"])
    gap_pct = (px - avg_cost) / avg_cost * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 시장가 (실시간)",      f"${px:,.2f}")
    c2.metric("기관 순수 평단가",          f"${avg_cost:,.2f}")
    c3.metric("평단가 대비 괴리율",        f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
    c4.metric(f"{date_str} 순매수(BTC)",  f"{latest['flow_btc_final']:,.4f}")
    c5.metric("추정 보유량 (BTC)",         f"{holdings:,.2f}")

    st.subheader("평단가 vs 현재가 추세")
    chart = df[["date", "implied_btc_px", "avg_buy_price_ex_fee"]].copy()
    chart.loc[chart.index[-1], "implied_btc_px"] = px
    chart = chart.set_index("date")
    chart.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart, color=["#FFFFFF", "#FF8C00"])


st.title("ETF 추적 대시보드")
st.markdown("비트와이즈와 블랙록의 추정 평단가와 자금 흐름을 추적합니다.")

ic1, ic2 = st.columns(2)
ic1.info("📅 기관 데이터(평단가 · 보유량 · 자금흐름): 매일 오후 12시(KST) 자동 최신화")
ic2.success("⚡ 현재 시장가: CoinGecko 실시간 조회 · 10초마다 자동 갱신")

tab1, tab2 = st.tabs(["솔라나 (BSOL)", "비트코인 (IBIT)"])

with tab1:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    if not df_bsol.empty:
        bsol_live(df_bsol)
        st.subheader("기관 자금 흐름 (Flow)")
        flow = df_bsol[["date", "flow_sol_final"]].copy()
        flow["color"] = flow["flow_sol_final"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
        st.bar_chart(flow, x="date", y="flow_sol_final", color="color")
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

with tab2:
    st.header("IBIT (iShares Bitcoin Trust)")
    df_ibit = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    if not df_ibit.empty:
        ibit_live(df_ibit)
        st.subheader("기관 자금 흐름 (Flow)")
        flow = df_ibit[["date", "flow_btc_final"]].copy()
        flow["color"] = flow["flow_btc_final"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
        st.bar_chart(flow, x="date", y="flow_btc_final", color="color")
    else:
        st.warning("IBIT 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")
