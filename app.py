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

@st.cache_data(ttl=300)
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
col_info2.success("⚡ 현재 시장가: Yahoo Finance 조회 · 5분마다 자동 갱신")

# ── 탭 ────────────────────────────────────────────────────────
tab_ibit, tab_etha, tab_bsol = st.tabs(["비트코인 (IBIT)", "이더리움 (ETHA)", "솔라나 (BSOL)"])

# ══════════════════════════════════════════════════════════════
# IBIT 탭
# ══════════════════════════════════════════════════════════════
@st.fragment(run_every=300)
def ibit_live(df: pd.DataFrame) -> None:
    btc_px = get_crypto_prices()["BTC"]
    px_str = f"${btc_px:,.2f}" if btc_px else "N/A"

    has_prev      = len(df) >= 2
    latest        = df.iloc[-1]
    prev          = df.iloc[-2] if has_prev else latest
    avg_cost      = float(latest["avg_buy_price_ex_fee"])
    prev_cost     = float(prev["avg_buy_price_ex_fee"])
    btc_held      = float(latest.get("btc_in_trust", 0) or 0)
    prev_held     = float(prev.get("btc_in_trust", 0) or 0)
    flow_val      = float(latest["flow_btc_final"])
    prev_flow_val = float(prev.get("flow_btc_final", 0) or 0)
    # T+1 결제 구조: 최신 행의 flow는 전 영업일 거래분 → 이전 행 날짜로 표기
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    prev_btc_px = st.session_state.get("prev_btc_px")

    if btc_px and avg_cost > 0:
        gap_pct = (btc_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
        if prev_btc_px and avg_cost > 0:
            prev_gap_pct = (prev_btc_px - avg_cost) / avg_cost * 100
            gap_delta = f"{gap_pct - prev_gap_pct:+.2f}%p"
        else:
            gap_delta = None
    else:
        gap_str   = "N/A"
        gap_delta = None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 BTC 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"${avg_cost - prev_cost:+,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str, delta=gap_delta)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} BTC",
              delta=f"{flow_val - prev_flow_val:+,.4f} BTC" if has_prev else None)
    c5.metric("추정 BTC 보유량",    f"{btc_held:,.2f} BTC",
              delta=f"{btc_held - prev_held:+,.2f} BTC" if has_prev else None)

    st.session_state["prev_btc_px"] = btc_px

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="ibit_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])

    df_chart = df[["date", "implied_btc_px", "avg_buy_price_ex_fee"]].copy()
    df_chart["MA7"]   = df_chart["implied_btc_px"].rolling(7).mean()
    df_chart["MA200"] = df_chart["implied_btc_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA7", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FF8C00", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (BTC)")
    st.altair_chart(flow_chart(df_flow, "flow_btc_final"), use_container_width=True)

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
@st.fragment(run_every=300)
def etha_live(df: pd.DataFrame) -> None:
    eth_px = get_crypto_prices()["ETH"]
    px_str = f"${eth_px:,.2f}" if eth_px else "N/A"

    has_prev      = len(df) >= 2
    latest        = df.iloc[-1]
    prev          = df.iloc[-2] if has_prev else latest
    avg_cost      = float(latest["avg_buy_price_ex_fee"])
    prev_cost     = float(prev["avg_buy_price_ex_fee"])
    eth_held      = float(latest.get("eth_in_trust", 0) or 0)
    prev_held     = float(prev.get("eth_in_trust", 0) or 0)
    flow_val      = float(latest["flow_eth_final"])
    prev_flow_val = float(prev.get("flow_eth_final", 0) or 0)
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    prev_eth_px = st.session_state.get("prev_eth_px")

    if eth_px and avg_cost > 0:
        gap_pct = (eth_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
        if prev_eth_px and avg_cost > 0:
            prev_gap_pct = (prev_eth_px - avg_cost) / avg_cost * 100
            gap_delta = f"{gap_pct - prev_gap_pct:+.2f}%p"
        else:
            gap_delta = None
    else:
        gap_str   = "N/A"
        gap_delta = None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 ETH 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"${avg_cost - prev_cost:+,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str, delta=gap_delta)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} ETH",
              delta=f"{flow_val - prev_flow_val:+,.4f} ETH" if has_prev else None)
    c5.metric("추정 ETH 보유량",    f"{eth_held:,.2f} ETH",
              delta=f"{eth_held - prev_held:+,.2f} ETH" if has_prev else None)

    st.session_state["prev_eth_px"] = eth_px

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="etha_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])

    df_chart = df[["date", "implied_eth_px", "avg_buy_price_ex_fee"]].copy()
    df_chart["MA7"]   = df_chart["implied_eth_px"].rolling(7).mean()
    df_chart["MA200"] = df_chart["implied_eth_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA7", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FFD700", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (ETH)")
    st.altair_chart(flow_chart(df_flow, "flow_eth_final"), use_container_width=True)

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
@st.fragment(run_every=300)
def bsol_live(df: pd.DataFrame) -> None:
    sol_px = get_crypto_prices()["SOL"]
    px_str = f"${sol_px:,.2f}" if sol_px else "N/A"

    has_prev      = len(df) >= 2
    latest        = df.iloc[-1]
    prev          = df.iloc[-2] if has_prev else latest
    avg_cost      = float(latest["avg_buy_price_ex_staking"])
    prev_cost     = float(prev["avg_buy_price_ex_staking"])
    sol_held      = float(latest.get("sol_in_trust", 0) or 0)
    prev_held     = float(prev.get("sol_in_trust", 0) or 0)
    flow_val      = float(latest["flow_sol_final"])
    prev_flow_val = float(prev.get("flow_sol_final", 0) or 0)
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    prev_sol_px = st.session_state.get("prev_sol_px")

    if sol_px and avg_cost > 0:
        gap_pct = (sol_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
        if prev_sol_px and avg_cost > 0:
            prev_gap_pct = (prev_sol_px - avg_cost) / avg_cost * 100
            gap_delta = f"{gap_pct - prev_gap_pct:+.2f}%p"
        else:
            gap_delta = None
    else:
        gap_str   = "N/A"
        gap_delta = None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 SOL 시장가",    px_str)
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"${avg_cost - prev_cost:+,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str, delta=gap_delta)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} SOL",
              delta=f"{flow_val - prev_flow_val:+,.4f} SOL" if has_prev else None)
    c5.metric("추정 SOL 보유량",    f"{sol_held:,.2f} SOL",
              delta=f"{sol_held - prev_held:+,.2f} SOL" if has_prev else None)

    st.session_state["prev_sol_px"] = sol_px

    st.subheader("스테이킹 현황")
    staking_rate = float(latest.get("net_staking_reward_rate_pct") or 0)
    sponsor_fee  = float(latest.get("sponsor_fee_pct") or 0)
    cum_reward   = float(latest.get("cumulative_est_staking_sol") or 0)
    obs_yield    = latest.get("observed_annual_yield_pct")
    obs_yield_str = f"{float(obs_yield):.2f}%" if obs_yield and str(obs_yield) not in ("", "nan") else "N/A"
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("스테이킹 APR",      f"{staking_rate:.2f}%")
    s2.metric("스폰서 피",          f"{sponsor_fee:.2f}%")
    s3.metric("누적 스테이킹 보상", f"{cum_reward:,.2f} SOL")
    s4.metric("관측 연간 수익률",   obs_yield_str)

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="bsol_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])

    df_chart = df[["date", "implied_sol_px", "avg_buy_price_ex_staking"]].copy()
    df_chart["MA7"]   = df_chart["implied_sol_px"].rolling(7).mean()
    df_chart["MA200"] = df_chart["implied_sol_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA7", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FFD700", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (SOL)")
    st.altair_chart(flow_chart(df_flow, "flow_sol_final"), use_container_width=True)

with tab_bsol:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    if not df_bsol.empty:
        bsol_live(df_bsol)
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

# ── 푸터 ──────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#888; font-size:0.8em;'>"
    "본 대시보드는 정보 제공 목적의 개인 프로젝트이며, 투자 조언이 아닙니다. · "
    "This dashboard is for informational purposes only and does not constitute investment advice.<br>"
    "자세한 내용은 <a href='https://jungmin1212.github.io/etf-dashboard/etf-dashboard.html' target='_blank'>법적 고지 · Legal Notice</a>를 참조하세요."
    "</div>",
    unsafe_allow_html=True,
)

