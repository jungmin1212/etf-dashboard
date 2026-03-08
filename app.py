import streamlit as st
import pandas as pd
import altair as alt
import yfinance as yf
from pathlib import Path

# ── 페이지 설정 ────────────────────────────────────────────────
st.set_page_config(page_title="크립토 ETF 퀀트 대시보드", layout="wide")

PERIOD_DAYS = {"30일": 30, "90일": 90, "120일": 120, "1년": 365}

# ── 모바일 반응형 CSS ──────────────────────────────────────────
st.markdown("""
<style>
@media (max-width: 768px) {
    /* 5열 메트릭 → 2열로 줄바꿈 */
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }
    [data-testid="column"] {
        min-width: 47% !important;
        flex: 1 1 47% !important;
    }
    /* 라디오 버튼 세로로 */
    [data-testid="stRadio"] > div {
        flex-direction: column !important;
    }
    /* 폰트 크기 조금 줄이기 */
    [data-testid="metric-container"] {
        font-size: 0.85em;
    }
}
</style>
""", unsafe_allow_html=True)

# ── 데이터 로딩 ────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data(file_name: str) -> pd.DataFrame:
    fp = Path(file_name)
    if fp.exists():
        df = pd.read_csv(fp)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame()

@st.cache_data(ttl=60)
def get_crypto_prices() -> dict:
    prices = {"SOL": None, "BTC": None, "ETH": None}
    try:
        prices["SOL"] = yf.Ticker("SOL-USD").fast_info.last_price
        prices["BTC"] = yf.Ticker("BTC-USD").fast_info.last_price
        prices["ETH"] = yf.Ticker("ETH-USD").fast_info.last_price
        if all(v is not None for v in prices.values()):
            return prices
    except Exception:
        pass
    # 백업: CoinGecko
    try:
        import requests
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana,bitcoin,ethereum", "vs_currencies": "usd"},
            timeout=10,
        )
        data = r.json()
        prices["SOL"] = data.get("solana",   {}).get("usd")
        prices["BTC"] = data.get("bitcoin",  {}).get("usd")
        prices["ETH"] = data.get("ethereum", {}).get("usd")
    except Exception:
        pass
    return prices

# ── Altair 자금 흐름 차트 ──────────────────────────────────────
def flow_chart(df: pd.DataFrame, col: str, color_pos="#2ecc71", color_neg="#e74c3c"):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="날짜"),
            y=alt.Y(f"{col}:Q", title="자금 흐름"),
            color=alt.condition(
                alt.datum[col] >= 0,
                alt.value(color_pos),
                alt.value(color_neg),
            ),
            tooltip=["date:T", f"{col}:Q"],
        )
    )
    return chart

# ── 헤더 ──────────────────────────────────────────────────────
st.title("ETF 추적 대시보드")
st.markdown("블랙록(IBIT·ETHA)과 비트와이즈(BSOL) 추정 평단가와 자금 흐름을 추적합니다.")
col_info1, col_info2 = st.columns(2)
col_info1.info("📊 기관 데이터(평단가·보유량·자금흐름): 매일 오후 12시(KST) 자동 최신화")
col_info2.success("⚡ 현재 시장가: Yahoo Finance 실시간 조회 · 1분마다 자동 갱신")

# ── 탭 ────────────────────────────────────────────────────────
tab_ibit, tab_etha, tab_bsol = st.tabs(["비트코인 (IBIT)", "이더리움 (ETHA)", "솔라나 (BSOL)"])

# ══════════════════════════════════════════════════════════════
# IBIT 탭
# ══════════════════════════════════════════════════════════════
@st.fragment(run_every=60)
def ibit_live(df: pd.DataFrame) -> None:
    btc_px = get_crypto_prices()["BTC"]
    px_str = f"${btc_px:,.2f}" if btc_px else "N/A"

    latest = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_fee"])
    btc_held = float(latest.get("btc_in_trust", 0) or 0)
    date_str = latest["date"].strftime("%m월 %d일")

    if btc_px and avg_cost > 0:
        gap_pct = (btc_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 BTC 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}")
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{float(latest['flow_btc_final']):,.4f} BTC")
    c5.metric("추정 BTC 보유량",    f"{btc_held:,.2f} BTC")

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="ibit_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])
    df_f   = df[df["date"] >= cutoff].reset_index(drop=True)

    st.subheader("평단가 vs 현재가 추세")
    chart_data = df_f[["date", "implied_btc_px", "avg_buy_price_ex_fee"]].set_index("date")
    chart_data.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart_data, color=["#FFFFFF", "#FF8C00"])

    st.subheader("기관 자금 흐름 (BTC)")
    st.altair_chart(flow_chart(df_f, "flow_btc_final"), use_container_width=True)

with tab_ibit:
    st.header("IBIT (iShares Bitcoin Trust)")
    df_ibit = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    if not df_ibit.empty:
        ibit_live(df_ibit)
    else:
        st.warning("IBIT 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

# ══════════════════════════════════════════════════════════════
# ETHA 탭
# ══════════════════════════════════════════════════════════════
@st.fragment(run_every=60)
def etha_live(df: pd.DataFrame) -> None:
    eth_px = get_crypto_prices()["ETH"]
    px_str = f"${eth_px:,.2f}" if eth_px else "N/A"

    latest = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_fee"])
    eth_held = float(latest.get("eth_in_trust", 0) or 0)
    date_str = latest["date"].strftime("%m월 %d일")

    if eth_px and avg_cost > 0:
        gap_pct = (eth_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 ETH 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}")
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{float(latest['flow_eth_final']):,.4f} ETH")
    c5.metric("추정 ETH 보유량",    f"{eth_held:,.2f} ETH")

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="etha_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])
    df_f   = df[df["date"] >= cutoff].reset_index(drop=True)

    st.subheader("평단가 vs 현재가 추세")
    chart_data = df_f[["date", "implied_eth_px", "avg_buy_price_ex_fee"]].set_index("date")
    chart_data.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart_data, color=["#FFFFFF", "#627EEA"])

    st.subheader("기관 자금 흐름 (ETH)")
    st.altair_chart(flow_chart(df_f, "flow_eth_final"), use_container_width=True)

with tab_etha:
    st.header("ETHA (iShares Ethereum Trust)")
    df_etha = load_data("etha_tracker/etha_cost_basis_track.csv")
    if not df_etha.empty:
        etha_live(df_etha)
    else:
        st.warning("ETHA 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

# ══════════════════════════════════════════════════════════════
# BSOL 탭
# ══════════════════════════════════════════════════════════════
@st.fragment(run_every=60)
def bsol_live(df: pd.DataFrame) -> None:
    sol_px = get_crypto_prices()["SOL"]
    px_str = f"${sol_px:,.2f}" if sol_px else "N/A"

    latest = df.iloc[-1]
    avg_cost = float(latest["avg_buy_price_ex_staking"])
    sol_held = float(latest.get("sol_in_trust", 0) or 0)
    date_str = latest["date"].strftime("%m월 %d일")

    if sol_px and avg_cost > 0:
        gap_pct = (sol_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 SOL 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}")
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{float(latest['flow_sol_final']):,.4f} SOL")
    c5.metric("추정 SOL 보유량",    f"{sol_held:,.2f} SOL")

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="bsol_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])
    df_f   = df[df["date"] >= cutoff].reset_index(drop=True)

    st.subheader("평단가 vs 현재가 추세")
    chart_data = df_f[["date", "implied_sol_px", "avg_buy_price_ex_staking"]].set_index("date")
    chart_data.columns = ["시장가 (Market Price)", "기관 평단가 (Cost Basis)"]
    st.line_chart(chart_data, color=["#FFFFFF", "#FFD700"])

    st.subheader("기관 자금 흐름 (SOL)")
    st.altair_chart(flow_chart(df_f, "flow_sol_final"), use_container_width=True)

with tab_bsol:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    if not df_bsol.empty:
        bsol_live(df_bsol)
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")
