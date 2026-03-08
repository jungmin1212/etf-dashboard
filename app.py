import math

import altair as alt
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
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


@st.cache_data(ttl=60)
def get_crypto_prices() -> dict:
    result = {"SOL": None, "BTC": None}

    try:
        sol = yf.Ticker("SOL-USD").fast_info.last_price
        btc = yf.Ticker("BTC-USD").fast_info.last_price
        if sol and not math.isnan(float(sol)):
            result["SOL"] = float(sol)
        if btc and not math.isnan(float(btc)):
            result["BTC"] = float(btc)
    except Exception:
        pass

    if result["SOL"] is None or result["BTC"] is None:
        try:
            resp = requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "solana,bitcoin", "vs_currencies": "usd"},
                headers={"accept": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if result["SOL"] is None:
                result["SOL"] = float(data["solana"]["usd"])
            if result["BTC"] is None:
                result["BTC"] = float(data["bitcoin"]["usd"])
        except Exception:
            pass

    return result


def flow_chart(df_f: pd.DataFrame, col: str) -> alt.Chart:
    """양수=초록, 음수=빨강 — 값 자체로 판단하므로 인덱스 무관"""
    return (
        alt.Chart(df_f)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y(f"{col}:Q", title=col),
            color=alt.condition(
                alt.datum[col] >= 0,
                alt.value("#2ecc71"),   # 양수 → 초록
                alt.value("#e74c3c"),   # 음수 → 빨강
            ),
        )
        .properties(height=300)
    )


PERIOD_DAYS = {"30일": 30, "90일": 90, "120일": 120, "1년": 365}


@st.fragment(run_every=60)
def bsol_live(df: pd.DataFrame) -> None:
    latest   = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_staking"])
    holdings = float(latest["sol_in_trust"])
    date_str = latest["date"].strftime("%m월 %d일")

    sol_px = get_crypto_prices()["SOL"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c2.metric("기관 순수 평단가",         f"${avg_cost:,.2f}")
    c4.metric(f"{date_str} 순매수(SOL)", f"{latest['flow_sol_final']:,.2f}")
    c5.metric("추정 보유량 (SOL)",        f"{holdings:,.0f}")

    if sol_px is not None:
        gap_pct = (sol_px - avg_cost) / avg_cost * 100
        c1.metric("현재 시장가 (실시간)", f"${sol_px:,.2f}")
        c3.metric("평단가 대비 괴리율",   f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
    else:
        c1.metric("현재 시장가 (실시간)", "N/A")
        c3.metric("평단가 대비 괴리율",   "N/A")

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="bsol_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])
    df_f   = df[df["date"] >= cutoff].reset_index(drop=True)

    st.subheader("평단가 vs 현재가 추세")
    chart = df_f[["date", "implied_sol_px", "avg_buy_price_ex_staking"]].copy()
    if sol_px is not None:
        chart.loc[chart.index[-1], "implied_sol_px"] = sol_px
    chart = chart.set_index("date")
    chart.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart, color=["#FFFFFF", "#FFD700"])

    st.subheader("기관 자금 흐름 (Flow)")
    st.altair_chart(flow_chart(df_f, "flow_sol_final"), use_container_width=True)


@st.fragment(run_every=60)
def ibit_live(df: pd.DataFrame) -> None:
    latest   = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_fee"])
    holdings = float(latest["btc_in_trust"])
    date_str = latest["date"].strftime("%m월 %d일")

    btc_px = get_crypto_prices()["BTC"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c2.metric("기관 순수 평단가",         f"${avg_cost:,.2f}")
    c4.metric(f"{date_str} 순매수(BTC)", f"{latest['flow_btc_final']:,.4f}")
    c5.metric("추정 보유량 (BTC)",        f"{holdings:,.2f}")

    if btc_px is not None:
        gap_pct = (btc_px - avg_cost) / avg_cost * 100
        c1.metric("현재 시장가 (실시간)", f"${btc_px:,.2f}")
        c3.metric("평단가 대비 괴리율",   f"{gap_pct:.2f}%", f"{gap_pct:.2f}%")
    else:
        c1.metric("현재 시장가 (실시간)", "N/A")
        c3.metric("평단가 대비 괴리율",   "N/A")

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="ibit_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])
    df_f   = df[df["date"] >= cutoff].reset_index(drop=True)

    st.subheader("평단가 vs 현재가 추세")
    chart = df_f[["date", "implied_btc_px", "avg_buy_price_ex_fee"]].copy()
    if btc_px is not None:
        chart.loc[chart.index[-1], "implied_btc_px"] = btc_px
    chart = chart.set_index("date")
    chart.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart, color=["#FFFFFF", "#FF8C00"])

    st.subheader("기관 자금 흐름 (Flow)")
    st.altair_chart(flow_chart(df_f, "flow_btc_final"), use_container_width=True)


# ── 메인 레이아웃 ─────────────────────────────────────────────
st.title("ETF 추적 대시보드")
st.markdown("블랙록과 비트와이즈 추정 평단가와 자금 흐름을 추적합니다.")

ic1, ic2 = st.columns(2)
ic1.info("📅 기관 데이터(평단가 · 보유량 · 자금흐름): 매일 오후 12시(KST) 자동 최신화")
ic2.success("⚡ 현재 시장가: Yahoo Finance 실시간 조회 · 1분마다 자동 갱신")

tab1, tab2 = st.tabs(["비트코인 (IBIT)", "솔라나 (BSOL)"])

with tab1:
    st.header("IBIT (iShares Bitcoin Trust)")
    df_ibit = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    if not df_ibit.empty:
        ibit_live(df_ibit)
    else:
        st.warning("IBIT 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

with tab2:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    if not df_bsol.empty:
        bsol_live(df_bsol)
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")
