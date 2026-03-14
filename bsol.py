# bsol_avg_cost_tracker.py

import re
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from utils import to_float, find_first, fetch_page_text, HEADERS

URL = "https://bsoletf.com"
DATA_DIR = Path("bsol_tracker")
SNAPSHOT_CSV = DATA_DIR / "bsol_daily_snapshots.csv"
TRACK_CSV = DATA_DIR / "bsol_cost_basis_track.csv"

# 과거 백필 없이 오늘부터 추적할 경우 None 유지
# 과거 초기 평단을 따로 알고 있으면 숫자(USD per SOL)로 넣기
SEED_AVG_COST = None

# ── Wayback Machine 백필 데이터 (bsoletf.com 아카이브 스냅샷, 수동 추출) ──────
# sponsor_fee: 출시 후 3개월(~2026-02) 면제 조항 반영 → 0.0
_WAYBACK_ROWS = [
    {
        "date":                       "2025-11-06",
        "obs_ts_utc":                 "2025-11-06T09:08:04+00:00",
        "sol_in_trust":               2_670_590.27,
        "market_value_usd":           435_194_850.57,
        "sol_per_share":              0.130783,
        "nav_usd":                    21.31,
        "market_price_usd":           20.07,
        "premium_discount_pct":       round((20.07 - 21.31) / 21.31 * 100, 2),
        "aum_usd":                    435_194_851.0,
        "shares_outstanding":         20_420_000.0,
        "sponsor_fee_pct":            0.0,
        "net_staking_reward_rate_pct": 7.25,
    },
    {
        "date":                       "2025-11-08",
        "obs_ts_utc":                 "2025-11-08T18:41:24+00:00",
        "sol_in_trust":               2_931_392.0,
        "market_value_usd":           21.35 * 22_400_000.0,
        "sol_per_share":              0.130866,
        "nav_usd":                    21.35,
        "market_price_usd":           20.33,
        "premium_discount_pct":       round((20.33 - 21.35) / 21.35 * 100, 2),
        "aum_usd":                    np.nan,
        "shares_outstanding":         22_400_000.0,
        "sponsor_fee_pct":            0.0,
        "net_staking_reward_rate_pct": 7.22,
    },
    {
        "date":                       "2025-11-17",
        "obs_ts_utc":                 "2025-11-17T04:40:40+00:00",
        "sol_in_trust":               3_160_303.72,
        "market_value_usd":           18.52 * 24_120_000.0,
        "sol_per_share":              0.131024,
        "nav_usd":                    18.52,
        "market_price_usd":           18.68,
        "premium_discount_pct":       round((18.68 - 18.52) / 18.52 * 100, 2),
        "aum_usd":                    np.nan,
        "shares_outstanding":         24_120_000.0,
        "sponsor_fee_pct":            0.0,
        "net_staking_reward_rate_pct": 7.12,
    },
    {
        "date":                       "2026-01-02",
        "obs_ts_utc":                 "2026-01-02T14:45:02+00:00",
        "sol_in_trust":               5_174_316.62,
        "market_value_usd":           16.35 * 39_180_000.0,
        "sol_per_share":              0.132065,
        "nav_usd":                    16.35,
        "market_price_usd":           16.40,
        "premium_discount_pct":       round((16.40 - 16.35) / 16.35 * 100, 2),
        "aum_usd":                    np.nan,
        "shares_outstanding":         39_180_000.0,
        "sponsor_fee_pct":            0.0,
        "net_staking_reward_rate_pct": 6.76,
    },
    {
        "date":                       "2026-01-17",
        "obs_ts_utc":                 "2026-01-17T03:44:51+00:00",
        "sol_in_trust":               5_581_240.53,
        "market_value_usd":           19.09 * 42_130_000.0,
        "sol_per_share":              0.132477,
        "nav_usd":                    19.09,
        "market_price_usd":           18.71,
        "premium_discount_pct":       round((18.71 - 19.09) / 19.09 * 100, 2),
        "aum_usd":                    np.nan,
        "shares_outstanding":         42_130_000.0,
        "sponsor_fee_pct":            0.0,
        "net_staking_reward_rate_pct": 6.74,
    },
]


def parse_snapshot(text):
    # 1. 날짜 형식 유연화
    data_as_of_all = re.findall(r"Data as of\s*(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})", text, re.I)
    if data_as_of_all:
        asof = pd.to_datetime(data_as_of_all).max().date().isoformat()
    else:
        asof = datetime.now(timezone.utc).date().isoformat()

    snapshot = {
        "date": asof,
        "obs_ts_utc": datetime.now(timezone.utc).isoformat(),
        "sol_in_trust": find_first(text, [
            r"Solana in Trust\s*([\d,]+(?:\.\d+)?)",
            r"Total SOL in Trust\s*([\d,]+(?:\.\d+)?)",
            r"SOL in Trust\s*([\d,]+(?:\.\d+)?)"
        ]),
        "market_value_usd": find_first(text, [
            r"Market Value\s*\$([\d,]+(?:\.\d+)?)",
            r"Total Market Value\s*\$([\d,]+(?:\.\d+)?)"
        ]),
        "sol_per_share": find_first(text, [
            r"Solana per Share\s*([\d,]+(?:\.\d+)?)",
            r"SOL per Share\s*([\d,]+(?:\.\d+)?)"
        ]),
        "nav_usd": find_first(text, [
            r"NAV:\s*\$([\d,]+(?:\.\d+)?)",
            r"\bNAV\b\s*\$([\d,]+(?:\.\d+)?)",
            r"Net Asset Value.*\$([\d,]+(?:\.\d+)?)"
        ]),
        "market_price_usd": find_first(text, [
            r"Market Price:\s*\$([\d,]+(?:\.\d+)?)",
            r"\bMarket Price\b\s*\$([\d,]+(?:\.\d+)?)",
            r"Closing Price\s*\$([\d,]+(?:\.\d+)?)"
        ]),
        "premium_discount_pct": find_first(text, [
            r"Premium\s*/\s*Discount.*?([-\d.]+)%",
            r"Premium.*Discount.*?([-\d.]+)%"
        ]),
        "aum_usd": find_first(text, [
            r"Net Assets\s*\(AUM\)\s*\$([\d,]+(?:\.\d+)?)",
            r"\bAUM\b\s*\$([\d,]+(?:\.\d+)?)"
        ]),
        "shares_outstanding": find_first(text, [
            r"Shares Outstanding\s*([\d,]+(?:\.\d+)?)",
            r"Total Shares\s*([\d,]+(?:\.\d+)?)"
        ]),
        "sponsor_fee_pct": find_first(text, [
            r"Sponsor Fee\s*([\d.]+)%",
            r"Management Fee\s*([\d.]+)%"
        ]),
        "net_staking_reward_rate_pct": find_first(text, [
            r"Net Staking Reward Rate[^\d-]*([-\d.]+)%",
            r"Staking Reward Rate[^\d-]*([-\d.]+)%"
        ]),
    }

    # implied price fallback
    if np.isnan(snapshot["market_value_usd"]) and not np.isnan(snapshot["nav_usd"]) and not np.isnan(snapshot["shares_outstanding"]):
        snapshot["market_value_usd"] = snapshot["nav_usd"] * snapshot["shares_outstanding"]

    # 무결성 검증
    critical_fields = ["sol_in_trust", "nav_usd", "shares_outstanding"]
    missing_data = [field for field in critical_fields if np.isnan(snapshot[field])]

    if missing_data:
        print(f"\n[경고] 핵심 데이터 누락: {', '.join(missing_data)}")
        print("  웹사이트 구조 변경 의심 → CSV 업데이트 스킵\n")
        return None

    return snapshot


def backfill_from_wayback():
    """Wayback Machine 스냅샷 5건을 DataFrame으로 반환 (최초 1회 자동 실행)."""
    print("[백필] Wayback Machine 아카이브 데이터 적용 중...", flush=True)
    df = pd.DataFrame(_WAYBACK_ROWS)
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    print(f"[백필 완료] {len(df)}건 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


def save_snapshot(snapshot, backfill_df=None):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame([snapshot])
    parts = [p for p in [backfill_df, new_df] if p is not None and not p.empty]
    if SNAPSHOT_CSV.exists():
        parts.insert(0, pd.read_csv(SNAPSHOT_CSV))

    df = pd.concat(parts, ignore_index=True) if parts else new_df
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df = (df.sort_values(["date", "obs_ts_utc"])
            .drop_duplicates(subset=["date"], keep="last")
            .reset_index(drop=True))
    df.to_csv(SNAPSHOT_CSV, index=False)
    return df


def build_cost_basis_track(df, seed_avg_cost=None):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    num_cols = [
        "sol_in_trust", "market_value_usd", "sol_per_share", "nav_usd",
        "market_price_usd", "premium_discount_pct", "aum_usd",
        "shares_outstanding", "sponsor_fee_pct", "net_staking_reward_rate_pct"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)

    df["implied_sol_px"] = np.where(
        (df["market_value_usd"] > 0) & (df["sol_in_trust"] > 0),
        df["market_value_usd"] / df["sol_in_trust"],
        np.where(
            (df["nav_usd"] > 0) & (df["sol_per_share"] > 0),
            df["nav_usd"] / df["sol_per_share"],
            np.nan
        )
    )

    df["share_delta"] = df["shares_outstanding"].diff().fillna(0.0)
    df["sol_delta"] = df["sol_in_trust"].diff().fillna(0.0)

    df["sponsor_fee_pct"] = df["sponsor_fee_pct"].ffill().fillna(0.0)
    df["net_staking_reward_rate_pct"] = df["net_staking_reward_rate_pct"].ffill().fillna(0.0)
    df["premium_discount_pct"] = df["premium_discount_pct"].fillna(0.0)

    prev_sol = df["sol_in_trust"].shift(1).fillna(df["sol_in_trust"])
    day_gaps = df["date"].diff().dt.days.fillna(1).clip(lower=1)
    df["est_staking_reward_sol"] = prev_sol * (df["net_staking_reward_rate_pct"] / 100.0) * day_gaps / 365.0
    df["est_sponsor_fee_sol"] = prev_sol * (df["sponsor_fee_pct"] / 100.0) * day_gaps / 365.0
    df["est_net_reward_sol"] = df["est_staking_reward_sol"] - df["est_sponsor_fee_sol"]

    df["flow_usd_nav"] = df["share_delta"] * df["nav_usd"]
    df["flow_sol_from_shares"] = np.where(
        df["implied_sol_px"] > 0,
        df["flow_usd_nav"] / df["implied_sol_px"],
        0.0
    )

    df["flow_sol_from_holdings"] = df["sol_delta"] - df["est_net_reward_sol"]

    # 교차 검증: 두 흐름 추정 방법의 합의도
    flow_agreement = 1.0 - (abs(df["flow_sol_from_shares"] - df["flow_sol_from_holdings"]) /
                            (abs(df["flow_sol_from_shares"]) + abs(df["flow_sol_from_holdings"]) + 1e-12))
    df["flow_method_agreement"] = np.clip(flow_agreement, 0.0, 1.0)

    premium_abs = df["premium_discount_pct"].abs()
    w_shares = np.clip(1.0 - premium_abs / 2.0, 0.25, 1.0)
    df["w_shares"] = w_shares
    df["flow_sol_final"] = w_shares * df["flow_sol_from_shares"] + (1.0 - w_shares) * df["flow_sol_from_holdings"]

    if not df.empty:
        df.loc[0, ["est_staking_reward_sol", "est_sponsor_fee_sol", "est_net_reward_sol",
                   "flow_sol_from_shares", "flow_sol_from_holdings", "flow_sol_final"]] = 0.0

    cost_basis_usd = []
    avg_buy_ex_staking = []
    effective_cost_per_current_sol = []
    non_staking_inventory_sol = []
    cumulative_est_staking_sol = []
    confidence_score = []

    first_px = df["implied_sol_px"].dropna().iloc[0] if not df["implied_sol_px"].dropna().empty else 0
    seed_px = first_px if seed_avg_cost is None else float(seed_avg_cost)

    inv_non_staking = float(df.loc[0, "sol_in_trust"]) if not df.empty else 0.0
    cb_usd = inv_non_staking * seed_px

    for i, row in df.iterrows():
        if i > 0:
            px = row["implied_sol_px"]
            if np.isnan(px):
                px = cb_usd / inv_non_staking if inv_non_staking > 1e-12 else seed_px

            buy_sol = max(row["flow_sol_final"], 0.0)
            sell_sol = max(-row["flow_sol_final"], 0.0)

            prev_avg = cb_usd / inv_non_staking if inv_non_staking > 1e-12 else px

            cb_usd += buy_sol * px
            realized_remove = min(sell_sol, inv_non_staking)
            cb_usd -= realized_remove * prev_avg
            inv_non_staking = max(inv_non_staking + buy_sol - realized_remove, 0.0)

        total_sol = float(row["sol_in_trust"])
        est_staking_cum = max(total_sol - inv_non_staking, 0.0)

        resid = abs(row["sol_delta"] - (row["est_net_reward_sol"] + row["flow_sol_final"]))
        denom = max(abs(row["sol_delta"]), 1e-8)
        conf = 0.0 if i == 0 else max(0.0, min(1.0,
                    1.0 - resid / denom - abs(row["premium_discount_pct"]) / 20.0
                    - (1.0 - row["flow_method_agreement"]) * 0.3))

        cost_basis_usd.append(cb_usd)
        avg_buy_ex_staking.append(cb_usd / inv_non_staking if inv_non_staking > 1e-12 else np.nan)
        effective_cost_per_current_sol.append(cb_usd / total_sol if total_sol > 1e-12 else np.nan)
        non_staking_inventory_sol.append(inv_non_staking)
        cumulative_est_staking_sol.append(est_staking_cum)
        confidence_score.append(conf)

    df["flow_usd_final"] = df["flow_sol_final"] * df["implied_sol_px"]
    df["cumulative_flow_usd"] = df["flow_usd_final"].cumsum()
    df["cost_basis_usd"] = cost_basis_usd
    df["avg_buy_price_ex_staking"] = avg_buy_ex_staking
    df["effective_cost_per_current_sol"] = effective_cost_per_current_sol
    df["non_staking_inventory_sol"] = non_staking_inventory_sol
    df["cumulative_est_staking_sol"] = cumulative_est_staking_sol
    df["confidence_score_0_1"] = confidence_score

    df["observed_annual_yield_pct"] = df["sol_per_share"].pct_change() * (365.0 / day_gaps) * 100
    df["estimated_annual_yield_pct"] = df["net_staking_reward_rate_pct"] - df["sponsor_fee_pct"]
    df["yield_model_error_pct"] = df["observed_annual_yield_pct"] - df["estimated_annual_yield_pct"]

    return df


def main():
    print("BSOL 데이터 수집 중...", flush=True)

    # 1. 스크래핑
    text = fetch_page_text(URL, headers=HEADERS)
    snapshot = parse_snapshot(text)

    if snapshot is None:
        print("[중단] 핵심 데이터 누락으로 CSV 업데이트 스킵")
        return

    # 2. 스마트 스킵
    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if not existing.empty and existing["date"].iloc[-1] == snapshot["date"]:
            print(f"[스킵] 데이터 변경 없음 ({snapshot['date']}) - 재계산만 수행")
            snap_df = save_snapshot(snapshot)
            track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)
            _save_track(track_df)
            return

    # 3. Wayback 백필: CSV가 없거나 6행 미만이면 자동 실행
    wayback_df = None
    needs_backfill = True
    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if len(existing) >= 6:
            needs_backfill = False
    if needs_backfill:
        wayback_df = backfill_from_wayback()

    snap_df = save_snapshot(snapshot, backfill_df=wayback_df)
    track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)

    _save_track(track_df)
    _print_report(track_df)


def _save_track(track_df):
    cols = [
        "date",
        "sol_in_trust",
        "market_value_usd",
        "implied_sol_px",
        "nav_usd",
        "market_price_usd",
        "premium_discount_pct",
        "shares_outstanding",
        "share_delta",
        "sol_delta",
        "net_staking_reward_rate_pct",
        "sponsor_fee_pct",
        "est_net_reward_sol",
        "w_shares",
        "flow_method_agreement",
        "flow_sol_final",
        "flow_usd_final",
        "cumulative_flow_usd",
        "non_staking_inventory_sol",
        "cumulative_est_staking_sol",
        "cost_basis_usd",
        "avg_buy_price_ex_staking",
        "effective_cost_per_current_sol",
        "confidence_score_0_1",
        "observed_annual_yield_pct",
        "estimated_annual_yield_pct",
        "yield_model_error_pct",
        "obs_ts_utc",
    ]
    track_df[cols].to_csv(TRACK_CSV, index=False)


def _print_report(track_df):
    latest = track_df.iloc[-1]
    date_str = str(latest["date"].date())
    implied_px = round(float(latest["implied_sol_px"]), 2)
    avg_buy = round(float(latest["avg_buy_price_ex_staking"]), 2) if pd.notna(latest["avg_buy_price_ex_staking"]) else 0.0
    eff_cost = round(float(latest["effective_cost_per_current_sol"]), 2) if pd.notna(latest["effective_cost_per_current_sol"]) else 0.0

    print("\n" + "="*55)
    print(f"  BSOL 평단가 추적 리포트 ({date_str})")
    print("="*55)
    print(f"  현재 SOL 추정 시장가     : ${implied_px}")
    print(f"  펀드 순수 매수 평단가     : ${avg_buy} (스테이킹 제외)")
    print(f"  펀드 실질 평단가          : ${eff_cost} (스테이킹 포함)")
    print("-"*55)

    if avg_buy > 0:
        gap_pct = (implied_px - avg_buy) / avg_buy * 100
        gap_sign = "+" if gap_pct >= 0 else ""
        gap_label = "프리미엄" if gap_pct >= 0 else "디스카운트"
        print(f"  현재가 vs 평단가 괴리     : {gap_sign}{round(gap_pct, 2)}% ({gap_label})")

    flow_sol = round(float(latest["flow_sol_final"]), 2)
    flow_label = "순매수" if flow_sol >= 0 else "순매도"
    print(f"  오늘 펀드 {flow_label}          : {abs(flow_sol):,.2f} SOL")

    sol_delta = round(float(latest["sol_delta"]), 2)
    print(f"  오늘 SOL 보유량 변화      : {sol_delta:+,.2f} SOL")

    if len(track_df) >= 2:
        recent = track_df.tail(min(7, len(track_df)))
        cumul_flow = round(float(recent["flow_sol_final"].sum()), 2)
        cumul_label = "순매수" if cumul_flow >= 0 else "순매도"
        print(f"  최근 {len(recent)}일 누적 {cumul_label}      : {abs(cumul_flow):,.2f} SOL")

    print("-"*55)
    print(f"  [검증] 신뢰도 점수        : {round(float(latest['confidence_score_0_1']), 4)}")
    print(f"  [정보] 총 데이터 일수     : {len(track_df)}일")
    print("="*55 + "\n")


if __name__ == "__main__":
    main()
