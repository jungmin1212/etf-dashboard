import streamlit as st
import pandas as pd
import numpy as np
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
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }
    [data-testid="column"] {
        min-width: 47% !important;
        flex: 1 1 47% !important;
    }
    [data-testid="stRadio"] > div {
        flex-direction: column !important;
    }
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

# ── 데이터 기준일 표시 헬퍼 ───────────────────────────────────
def show_data_timestamp(df: pd.DataFrame):
    if df.empty:
        return
    latest_date = df.iloc[-1]["date"].strftime("%Y-%m-%d")
    obs_ts = str(df.iloc[-1].get("obs_ts_utc", ""))
    if obs_ts and obs_ts != "backfill" and len(obs_ts) >= 16:
        try:
            ts_utc = pd.to_datetime(obs_ts, utc=True)
            ts_kst = ts_utc.tz_convert("Asia/Seoul")
            collected = ts_kst.strftime("%Y-%m-%d %H:%M KST")
        except Exception:
            collected = obs_ts[:16] + " UTC"
        st.caption(f"데이터 기준: **{latest_date}** | 수집: {collected}")
    else:
        st.caption(f"데이터 기준: **{latest_date}**")

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
        .properties(width="container")
    )
    return chart

# ── 모멘텀 지표 헬퍼 (P2-1) ──────────────────────────────────
def show_momentum_metrics(df: pd.DataFrame):
    """7일/30일 평균 순유입(USD)과 유입 가속도를 표시한다."""
    if "flow_usd_final" not in df.columns or len(df) < 7:
        return
    flow_7d = df["flow_usd_final"].rolling(7, min_periods=1).mean()
    flow_30d = df["flow_usd_final"].rolling(30, min_periods=1).mean()
    flow_accel = flow_7d - flow_7d.shift(7)

    latest_7d = float(flow_7d.iloc[-1])
    latest_30d = float(flow_30d.iloc[-1])
    latest_accel = float(flow_accel.iloc[-1]) if pd.notna(flow_accel.iloc[-1]) else 0.0

    m1, m2, m3 = st.columns(3)
    m1.metric("7일 평균 순유입 (USD)", f"${latest_7d:,.0f}")
    m2.metric("30일 평균 순유입 (USD)", f"${latest_30d:,.0f}")
    m3.metric("유입 가속도", f"${latest_accel:,.0f}",
              delta="가속" if latest_accel > 0 else "감속")

# ── 프리미엄/디스카운트 추세 차트 (P2-2) ──────────────────────
def show_premium_trend(df: pd.DataFrame, cutoff):
    """프리미엄/디스카운트 일별 + 7일 이동평균 차트."""
    if "premium_discount_pct" not in df.columns:
        return
    df_p = df[["date", "premium_discount_pct"]].copy()
    df_p["7일 이동평균"] = df_p["premium_discount_pct"].rolling(7, min_periods=1).mean()
    df_p = df_p[df_p["date"] >= cutoff].reset_index(drop=True)
    df_p = df_p.rename(columns={"premium_discount_pct": "일별"})

    st.subheader("프리미엄/디스카운트 추세")
    base = alt.Chart(df_p).encode(x=alt.X("date:T", title="날짜"))
    bar = base.mark_bar(opacity=0.4).encode(
        y=alt.Y("일별:Q", title="프리미엄/디스카운트 (%)"),
        color=alt.condition(alt.datum["일별"] >= 0, alt.value("#2ecc71"), alt.value("#e74c3c")),
    )
    line = base.mark_line(color="#FFD700", strokeWidth=2).encode(
        y=alt.Y("7일 이동평균:Q"),
    )
    zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="gray", strokeDash=[4, 4]).encode(y="y:Q")
    st.altair_chart((bar + line + zero).properties(width="container"))

# ── 가격-흐름 다이버전스 시그널 (P2-4) ────────────────────────
def show_divergence_signal(df: pd.DataFrame, price_col: str, cutoff):
    """가격 변화와 자금흐름의 z-score 기반 다이버전스 점수."""
    if "flow_usd_final" not in df.columns or price_col not in df.columns or len(df) < 30:
        return

    df_d = df[["date", price_col, "flow_usd_final"]].copy()
    pct = df_d[price_col].pct_change()
    pct_mean = pct.rolling(30, min_periods=7).mean()
    pct_std = pct.rolling(30, min_periods=7).std().replace(0, np.nan)
    price_z = (pct - pct_mean) / pct_std

    flow_mean = df_d["flow_usd_final"].rolling(30, min_periods=7).mean()
    flow_std = df_d["flow_usd_final"].rolling(30, min_periods=7).std().replace(0, np.nan)
    flow_z = (df_d["flow_usd_final"] - flow_mean) / flow_std

    df_d["divergence"] = flow_z - price_z
    df_d = df_d[df_d["date"] >= cutoff].reset_index(drop=True)

    latest_div = float(df_d["divergence"].iloc[-1]) if pd.notna(df_d["divergence"].iloc[-1]) else 0.0

    st.subheader("가격-흐름 다이버전스")
    if latest_div > 0.5:
        st.success(f"강세 다이버전스: {latest_div:+.2f} (기관이 가격 대비 적극 매수 중)")
    elif latest_div < -0.5:
        st.error(f"약세 다이버전스: {latest_div:+.2f} (기관이 가격 대비 매도/관망 중)")
    else:
        st.info(f"중립: {latest_div:+.2f} (가격과 흐름이 일치)")

    base = alt.Chart(df_d).encode(x=alt.X("date:T", title="날짜"))
    area_pos = base.mark_area(opacity=0.3).encode(
        y=alt.Y("divergence:Q", title="다이버전스 점수"),
        color=alt.condition(alt.datum.divergence >= 0, alt.value("#2ecc71"), alt.value("#e74c3c")),
    )
    zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="gray", strokeDash=[4, 4]).encode(y="y:Q")
    st.altair_chart((area_pos + zero).properties(width="container"))

# ── 평단가 지지/저항 밴드 차트 (P2-5) ────────────────────────
def show_support_resistance(df: pd.DataFrame, price_col: str, cost_col: str, cutoff):
    """기관 평단가 +/-5% 밴드와 시장가 비교 차트."""
    if cost_col not in df.columns or price_col not in df.columns:
        return
    df_sr = df[["date", price_col, cost_col]].copy()
    df_sr["support"] = df_sr[cost_col] * 0.95
    df_sr["resistance"] = df_sr[cost_col] * 1.05
    df_sr = df_sr[df_sr["date"] >= cutoff].reset_index(drop=True)

    st.subheader("기관 평단가 지지/저항 밴드")
    base = alt.Chart(df_sr).encode(x=alt.X("date:T", title="날짜"))
    band = base.mark_area(opacity=0.15, color="#FF8C00").encode(
        y=alt.Y("support:Q", title="가격 (USD)"),
        y2="resistance:Q",
    )
    cost_line = base.mark_line(color="#FF8C00", strokeWidth=2, strokeDash=[6, 3]).encode(
        y=alt.Y(f"{cost_col}:Q"),
        tooltip=["date:T", f"{cost_col}:Q"],
    )
    price_line = base.mark_line(color="#FFFFFF", strokeWidth=2).encode(
        y=alt.Y(f"{price_col}:Q"),
        tooltip=["date:T", f"{price_col}:Q"],
    )
    st.altair_chart((band + cost_line + price_line).properties(width="container"))

# ── 신뢰도 점수 설명 (P2-6) ──────────────────────────────────
def show_confidence_explainer(df: pd.DataFrame):
    """신뢰도 점수의 의미와 감점 요인을 설명한다."""
    if "confidence_score_0_1" not in df.columns:
        return
    latest_conf = float(df.iloc[-1]["confidence_score_0_1"])
    with st.expander(f"신뢰도 점수: {latest_conf:.4f}"):
        st.markdown(f"""
**현재 신뢰도: {latest_conf:.4f}**

| 범위 | 해석 |
|------|------|
| 0.8 ~ 1.0 | 높은 신뢰도 (정상) |
| 0.5 ~ 0.8 | 중간 신뢰도 |
| 0.0 ~ 0.5 | 낮은 신뢰도 (데이터 검증 필요) |

**감점 요인:**
1. **잔차 (residual)**: 실제 보유량 변화와 추정 흐름의 차이가 클수록 감점
2. **프리미엄/디스카운트**: ETF 프리미엄이 클수록 감점 (premium / 20)
3. **방법 간 불일치**: 주식수 기반 흐름과 보유량 기반 흐름이 다를수록 감점 (최대 -0.3)
""")

# ── 헤더 ──────────────────────────────────────────────────────
st.title("ETF 추적 대시보드")
st.markdown("블랙록(IBIT/ETHA)과 비트와이즈(BSOL) 추정 평단가와 자금 흐름을 추적합니다.")
col_info1, col_info2 = st.columns(2)
col_info1.info("기관 데이터(평단가/보유량/자금흐름): 매일 오전 7시~오후 6시(KST) 자동 최신화")
col_info2.success("현재 시장가: Yahoo Finance 조회 / 5분마다 자동 갱신")

# ── 탭 ────────────────────────────────────────────────────────
tab_ibit, tab_etha, tab_bsol, tab_cross = st.tabs(
    ["비트코인 (IBIT)", "이더리움 (ETHA)", "솔라나 (BSOL)", "크로스 ETF 분석"]
)

# ══════════════════════════════════════════════════════════════
# IBIT 탭
# ══════════════════════════════════════════════════════════════
@st.fragment(run_every=300)
def ibit_live(df: pd.DataFrame) -> None:
    btc_px = get_crypto_prices()["BTC"]
    px_str = f"${btc_px:,.2f}" if btc_px else "N/A"

    has_prev  = len(df) >= 2
    latest    = df.iloc[-1]
    prev      = df.iloc[-2] if has_prev else latest
    avg_cost  = float(latest["avg_buy_price_ex_fee"])
    prev_cost = float(prev["avg_buy_price_ex_fee"])
    btc_held  = float(latest.get("btc_in_trust", 0) or 0)
    prev_held = float(prev.get("btc_in_trust", 0) or 0)
    flow_val  = float(latest["flow_btc_final"])
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    if btc_px and avg_cost > 0:
        gap_pct = (btc_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    show_data_timestamp(df)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 BTC 시장가",    px_str)
    _d = avg_cost - prev_cost
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"{'+' if _d >= 0 else '-'}${abs(_d):,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} BTC")
    c5.metric("추정 BTC 보유량",    f"{btc_held:,.2f} BTC",
              delta=f"{btc_held - prev_held:+,.2f} BTC" if has_prev else None)

    # P2-1: 모멘텀 지표
    show_momentum_metrics(df)

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="ibit_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])

    # P2-5: 지지/저항 밴드
    show_support_resistance(df, "implied_btc_px", "avg_buy_price_ex_fee", cutoff)

    df_chart = df[["date", "implied_btc_px", "avg_buy_price_ex_fee"]].copy()
    df_chart["MA20"]  = df_chart["implied_btc_px"].rolling(20).mean()
    df_chart["MA200"] = df_chart["implied_btc_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA20", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FF8C00", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (BTC)")
    st.altair_chart(flow_chart(df_flow, "flow_btc_final"))

    # P2-2: 프리미엄 추세
    show_premium_trend(df, cutoff)

    # P2-4: 다이버전스 시그널
    show_divergence_signal(df, "implied_btc_px", cutoff)

    # P2-6: 신뢰도 설명
    show_confidence_explainer(df)

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

    has_prev  = len(df) >= 2
    latest    = df.iloc[-1]
    prev      = df.iloc[-2] if has_prev else latest
    avg_cost  = float(latest["avg_buy_price_ex_fee"])
    prev_cost = float(prev["avg_buy_price_ex_fee"])
    eth_held  = float(latest.get("eth_in_trust", 0) or 0)
    prev_held = float(prev.get("eth_in_trust", 0) or 0)
    flow_val  = float(latest["flow_eth_final"])
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    if eth_px and avg_cost > 0:
        gap_pct = (eth_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    show_data_timestamp(df)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 ETH 시장가",    px_str)
    _d = avg_cost - prev_cost
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"{'+' if _d >= 0 else '-'}${abs(_d):,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} ETH")
    c5.metric("추정 ETH 보유량",    f"{eth_held:,.2f} ETH",
              delta=f"{eth_held - prev_held:+,.2f} ETH" if has_prev else None)

    # P2-1: 모멘텀 지표
    show_momentum_metrics(df)

    period = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="etha_period")
    cutoff = df["date"].max() - pd.Timedelta(days=PERIOD_DAYS[period])

    # P2-5: 지지/저항 밴드
    show_support_resistance(df, "implied_eth_px", "avg_buy_price_ex_fee", cutoff)

    df_chart = df[["date", "implied_eth_px", "avg_buy_price_ex_fee"]].copy()
    df_chart["MA20"]  = df_chart["implied_eth_px"].rolling(20).mean()
    df_chart["MA200"] = df_chart["implied_eth_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA20", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FFD700", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (ETH)")
    st.altair_chart(flow_chart(df_flow, "flow_eth_final"))

    # P2-2: 프리미엄 추세
    show_premium_trend(df, cutoff)

    # P2-4: 다이버전스 시그널
    show_divergence_signal(df, "implied_eth_px", cutoff)

    # P2-6: 신뢰도 설명
    show_confidence_explainer(df)

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

    has_prev  = len(df) >= 2
    latest    = df.iloc[-1]
    prev      = df.iloc[-2] if has_prev else latest
    avg_cost  = float(latest["avg_buy_price_ex_staking"])
    prev_cost = float(prev["avg_buy_price_ex_staking"])
    sol_held  = float(latest.get("sol_in_trust", 0) or 0)
    prev_held = float(prev.get("sol_in_trust", 0) or 0)
    flow_val  = float(latest["flow_sol_final"])
    flow_date = df.iloc[-2]["date"] if has_prev else latest["date"]
    date_str  = flow_date.strftime("%m월 %d일")

    if sol_px and avg_cost > 0:
        gap_pct = (sol_px - avg_cost) / avg_cost * 100
        gap_str = f"{gap_pct:+.2f}%"
    else:
        gap_str = "N/A"

    show_data_timestamp(df)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("현재 SOL 시장가",    px_str)
    _d = avg_cost - prev_cost
    c2.metric("기관 순수 평단가",    f"${avg_cost:,.2f}",
              delta=f"{'+' if _d >= 0 else '-'}${abs(_d):,.2f}" if has_prev else None)
    c3.metric("평단가 대비 괴리율",  gap_str)
    c4.metric(f"{date_str} 순매수", f"{flow_val:,.4f} SOL")
    c5.metric("추정 SOL 보유량",    f"{sol_held:,.2f} SOL",
              delta=f"{sol_held - prev_held:+,.2f} SOL" if has_prev else None)

    # P2-1: 모멘텀 지표
    show_momentum_metrics(df)

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

    # P2-5: 지지/저항 밴드
    show_support_resistance(df, "implied_sol_px", "avg_buy_price_ex_staking", cutoff)

    df_chart = df[["date", "implied_sol_px", "avg_buy_price_ex_staking"]].copy()
    df_chart["MA20"]  = df_chart["implied_sol_px"].rolling(20).mean()
    df_chart["MA200"] = df_chart["implied_sol_px"].rolling(200).mean()
    df_f = df_chart[df_chart["date"] >= cutoff].set_index("date")
    df_f.columns = ["시장가", "기관 평단가", "MA20", "MA200"]

    st.subheader("평단가 vs 현재가 추세")
    st.line_chart(df_f, color=["#FFFFFF", "#FFD700", "#00BFFF", "#FF6B6B"])

    df_flow = df[df["date"] >= cutoff].reset_index(drop=True)
    st.subheader("기관 자금 흐름 (SOL)")
    st.altair_chart(flow_chart(df_flow, "flow_sol_final"))

    # P2-2: 프리미엄 추세
    show_premium_trend(df, cutoff)

    # P2-4: 다이버전스 시그널
    show_divergence_signal(df, "implied_sol_px", cutoff)

    # P2-6: 신뢰도 설명
    show_confidence_explainer(df)

with tab_bsol:
    st.header("BSOL (Bitwise Solana Staking ETF)")
    df_bsol = load_data("bsol_tracker/bsol_cost_basis_track.csv")
    if not df_bsol.empty:
        bsol_live(df_bsol)
    else:
        st.warning("BSOL 데이터가 없습니다. 스크립트를 먼저 실행해 주세요.")

# ══════════════════════════════════════════════════════════════
# 크로스 ETF 분석 탭 (P2-3)
# ══════════════════════════════════════════════════════════════
with tab_cross:
    st.header("크로스 ETF 분석")
    st.markdown("3개 ETF의 USD 자금흐름을 비교하여 기관의 섹터 로테이션과 자금 집중도를 분석합니다.")

    df_ibit_c = load_data("ibit_tracker/ibit_cost_basis_track.csv")
    df_etha_c = load_data("etha_tracker/etha_cost_basis_track.csv")
    df_bsol_c = load_data("bsol_tracker/bsol_cost_basis_track.csv")

    has_flow = all(
        not d.empty and "flow_usd_final" in d.columns
        for d in [df_ibit_c, df_etha_c, df_bsol_c]
    )

    if has_flow:
        period_cross = st.radio("기간", list(PERIOD_DAYS.keys()), horizontal=True, key="cross_period")
        cutoff_cross = pd.Timestamp.now() - pd.Timedelta(days=PERIOD_DAYS[period_cross])

        # 각 ETF의 USD 흐름 합치기
        frames = []
        for label, d in [("IBIT (BTC)", df_ibit_c), ("ETHA (ETH)", df_etha_c), ("BSOL (SOL)", df_bsol_c)]:
            tmp = d[d["date"] >= cutoff_cross][["date", "flow_usd_final"]].copy()
            tmp["ETF"] = label
            frames.append(tmp)
        combined = pd.concat(frames, ignore_index=True)

        # 1. USD 자금흐름 비교 차트
        st.subheader("ETF별 USD 자금흐름 비교")
        cross_chart = (
            alt.Chart(combined)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="날짜"),
                y=alt.Y("flow_usd_final:Q", title="USD 자금흐름"),
                color=alt.Color("ETF:N", scale=alt.Scale(
                    domain=["IBIT (BTC)", "ETHA (ETH)", "BSOL (SOL)"],
                    range=["#FF8C00", "#FFD700", "#00BFFF"],
                )),
                xOffset="ETF:N",
                tooltip=["date:T", "ETF:N", "flow_usd_final:Q"],
            )
            .properties(width="container")
        )
        st.altair_chart(cross_chart)

        # 2. 누적 USD 유입 비교
        st.subheader("누적 USD 유입 비교")
        cum_frames = []
        for label, d in [("IBIT (BTC)", df_ibit_c), ("ETHA (ETH)", df_etha_c), ("BSOL (SOL)", df_bsol_c)]:
            if "cumulative_flow_usd" in d.columns:
                tmp = d[d["date"] >= cutoff_cross][["date", "cumulative_flow_usd"]].copy()
                tmp["ETF"] = label
                cum_frames.append(tmp)
        if cum_frames:
            cum_combined = pd.concat(cum_frames, ignore_index=True)
            cum_chart = (
                alt.Chart(cum_combined)
                .mark_line(strokeWidth=2)
                .encode(
                    x=alt.X("date:T", title="날짜"),
                    y=alt.Y("cumulative_flow_usd:Q", title="누적 USD 유입"),
                    color=alt.Color("ETF:N", scale=alt.Scale(
                        domain=["IBIT (BTC)", "ETHA (ETH)", "BSOL (SOL)"],
                        range=["#FF8C00", "#FFD700", "#00BFFF"],
                    )),
                    tooltip=["date:T", "ETF:N", "cumulative_flow_usd:Q"],
                )
                .properties(width="container")
            )
            st.altair_chart(cum_chart)

        # 3. 흐름 상관관계
        st.subheader("자금흐름 상관관계")

        # 날짜 기준 merge
        ibit_flow = df_ibit_c[["date", "flow_usd_final"]].rename(columns={"flow_usd_final": "IBIT"})
        etha_flow = df_etha_c[["date", "flow_usd_final"]].rename(columns={"flow_usd_final": "ETHA"})
        bsol_flow = df_bsol_c[["date", "flow_usd_final"]].rename(columns={"flow_usd_final": "BSOL"})

        merged = ibit_flow.merge(etha_flow, on="date", how="outer").merge(bsol_flow, on="date", how="outer")
        merged = merged.sort_values("date").set_index("date")

        col_7d, col_30d = st.columns(2)
        with col_7d:
            st.markdown("**7일 롤링 상관계수**")
            corr_7d = merged.rolling(7, min_periods=3).corr()
            # 최신 상관계수 추출
            last_date = merged.index[-1]
            try:
                corr_matrix_7d = corr_7d.loc[last_date]
                st.dataframe(corr_matrix_7d.round(3), use_container_width=True)
            except Exception:
                st.info("7일 상관계수 계산에 충분한 데이터가 없습니다.")

        with col_30d:
            st.markdown("**30일 롤링 상관계수**")
            corr_30d = merged.rolling(30, min_periods=7).corr()
            try:
                corr_matrix_30d = corr_30d.loc[last_date]
                st.dataframe(corr_matrix_30d.round(3), use_container_width=True)
            except Exception:
                st.info("30일 상관계수 계산에 충분한 데이터가 없습니다.")

    else:
        st.warning("크로스 분석을 위해 3개 ETF 모두의 데이터가 필요합니다. 각 트래커 스크립트를 먼저 실행해 주세요.")

# ── 푸터 ──────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#888; font-size:0.8em;'>"
    "본 대시보드는 정보 제공 목적의 개인 프로젝트이며, 투자 조언이 아닙니다."
    "</div>",
    unsafe_allow_html=True,
)
