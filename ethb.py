# ethb_avg_cost_tracker.py
# BlackRock iShares Staked Ethereum Trust ETF (ETHB) 펀드 ETH 평균 매수가 추적기

import re
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from utils import to_float, find_first, fetch_with_retry, fetch_page_text, HEADERS

URL          = "https://www.ishares.com/us/products/348532/ishares-staked-ethereum-trust-etf"
XLS_URL      = ("https://www.ishares.com/us/products/348532/fund/"
                "1521942788811.ajax?fileType=xls&fileName=iShares-Staked-Ethereum-Trust-ETF_fund&dataType=fund")
DATA_DIR     = Path("ethb_tracker")
SNAPSHOT_CSV = DATA_DIR / "ethb_daily_snapshots.csv"
TRACK_CSV    = DATA_DIR / "ethb_cost_basis_track.csv"
INCEPTION    = pd.Timestamp("2026-03-12")   # ETHB 상장일
# 출시 후 12개월 / $25억 이하 0.12%, 이후 0.25%
MGMT_FEE_PCT = 0.12

SEED_AVG_COST = None


# ── XLS에서 최신 NAV/주식수 가져오기 (Primary 소스) ────────────────────────────
def fetch_xls_latest():
    """iShares 공식 XLS에서 최신 행의 date, nav_usd, shares_outstanding 추출."""
    try:
        print("[XLS] iShares ETHB XLS 다운로드 중...", flush=True)
        r = fetch_with_retry(XLS_URL, headers=HEADERS, timeout=30)
        cells = re.findall(r'<ss:Data[^>]*>([^<]+)</ss:Data>', r.text)

        header_idx = next((i for i, c in enumerate(cells) if c.strip() == "As Of"), None)
        if header_idx is None:
            print("[XLS] 헤더를 찾지 못함 — HTML fallback")
            return None

        # XLS는 최신→과거 순 정렬 → 모든 행 파싱 후 최대 날짜 선택
        rows = []
        i = header_idx + 4
        while i + 3 < len(cells):
            try:
                d   = pd.to_datetime(cells[i].strip()).date().isoformat()
                nav = float(cells[i + 1].strip().replace(",", ""))
                shr = float(cells[i + 3].strip().replace(",", ""))
                rows.append({"date": d, "nav_usd": nav, "shares_outstanding": shr})
                i += 4
            except Exception:
                break

        if not rows:
            return None

        latest = max(rows, key=lambda r: r["date"])
        print(f"[XLS] 최신 데이터: {latest['date']} | NAV=${latest['nav_usd']:.2f} | 주식수={latest['shares_outstanding']:,.0f}")
        return latest
    except Exception as e:
        print(f"[XLS 실패] {e} — HTML fallback 사용")
        return None


# ── HTML에서 basket/closing/premium 가져오기 ──────────────────────────────────
def fetch_html_supplementary():
    """HTML 페이지에서 XLS에 없는 필드 추출."""
    text = fetch_page_text(URL, headers=HEADERS)

    date_m = re.search(r"NAV as of\s+([A-Za-z]+ \d{1,2},\s*\d{4})", text, re.I)
    asof = (pd.to_datetime(date_m.group(1)).date().isoformat()
            if date_m else datetime.now(timezone.utc).date().isoformat())

    data = {
        "date":                 asof,
        "net_assets_usd":       find_first(text, [r"Net Assets of Fund\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "basket_usd":           find_first(text, [r"Basket Amount\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "basket_eth":           find_first(text, [
                                    r"(?:Indicative )?Basket Ether(?:eum)? Amount\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)",
                                    r"Basket Ether(?:eum)? Amount\s+as of.*?\n\s*([\d,]+(?:\.\d+)?)",
                                ]),
        "closing_price_usd":    find_first(text, [r"Closing Price\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"]),
        "premium_discount_pct": find_first(text, [r"Premium/Discount\s*\nas of.*?\n\s*([-\d.]+)"]),
        # HTML fallback용
        "nav_usd":              find_first(text, [r"NAV as of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "shares_outstanding":   find_first(text, [r"Shares Outstanding\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"]),
    }
    return data


# ── 스냅샷 조립 ──────────────────────────────────────────────────────────────
def build_snapshot():
    """XLS(primary) + HTML(supplementary)로 오늘 스냅샷 생성."""
    xls_data  = fetch_xls_latest()
    html_data = fetch_html_supplementary()

    if xls_data:
        nav    = xls_data["nav_usd"]
        shares = xls_data["shares_outstanding"]
        asof   = xls_data["date"]
    else:
        nav    = html_data["nav_usd"]
        shares = html_data["shares_outstanding"]
        asof   = html_data["date"]

    basket_usd = html_data["basket_usd"]
    basket_eth = html_data["basket_eth"]

    eth_per_share = np.nan
    if not np.isnan(basket_eth) and not np.isnan(basket_usd) and not np.isnan(nav) and nav > 0:
        basket_shares = basket_usd / nav
        if basket_shares > 0:
            eth_per_share = basket_eth / basket_shares

    snap = {
        "date":                 asof,
        "obs_ts_utc":           datetime.now(timezone.utc).isoformat(),
        "net_assets_usd":       html_data["net_assets_usd"],
        "nav_usd":              nav,
        "closing_price_usd":    html_data["closing_price_usd"],
        "premium_discount_pct": html_data["premium_discount_pct"],
        "shares_outstanding":   shares,
        "basket_usd":           basket_usd,
        "basket_eth":           basket_eth,
        "eth_per_share":        eth_per_share,
        "management_fee_pct":   MGMT_FEE_PCT,
    }

    critical = ["nav_usd", "shares_outstanding", "eth_per_share"]
    missing = [f for f in critical if np.isnan(snap[f])]
    if missing:
        print(f"\n[경고] 핵심 데이터 누락: {', '.join(missing)}")
        print("  웹사이트 구조 변경 의심 → CSV 업데이트 스킵\n")
        return None

    return snap


# ── XLS 백필 ──────────────────────────────────────────────────────────────────
def backfill_from_ishares_xls(current_eth_per_share: float):
    """다중 앵커 포인트 보간법으로 과거 ETH/share를 계산한다.

    주의: ETHB는 스테이킹 ETH라 eth_per_share가 시간에 따라 증가하지만,
    상장 후 기간이 짧아 근사치로 fee-only 모델 사용.
    """
    print("[백필] iShares ETHB XLS 다운로드 중...", flush=True)
    r = fetch_with_retry(XLS_URL, headers=HEADERS, timeout=30)
    cells = re.findall(r'<ss:Data[^>]*>([^<]+)</ss:Data>', r.text)

    header_idx = next((i for i, c in enumerate(cells) if c.strip() == "As Of"), None)
    if header_idx is None:
        print("[백필 실패] XLS에서 데이터를 찾지 못했습니다.")
        return pd.DataFrame()

    rows = []
    i = header_idx + 4
    while i + 3 < len(cells):
        try:
            d   = pd.to_datetime(cells[i].strip()).date().isoformat()
            nav = float(cells[i + 1].strip().replace(",", ""))
            shr = float(cells[i + 3].strip().replace(",", ""))
            rows.append({"date": d, "nav_usd": nav, "shares_outstanding": shr})
            i += 4
        except Exception:
            break

    if not rows:
        print("[백필 실패] 파싱된 데이터 없음.")
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    daily_factor = 1.0 + (MGMT_FEE_PCT / 100.0) / 365.0

    ref_date = pd.Timestamp(datetime.now(timezone.utc).date())
    anchors = {ref_date.isoformat()[:10]: current_eth_per_share}

    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        live = existing[existing["obs_ts_utc"] != "backfill"]
        if not live.empty and "eth_per_share" in live.columns:
            for _, row in live.iterrows():
                eps = pd.to_numeric(row.get("eth_per_share"), errors="coerce")
                if pd.notna(eps) and eps > 0:
                    anchors[str(row["date"])[:10]] = float(eps)

    anchor_dates = sorted(anchors.keys())
    print(f"[백필] 앵커 포인트 {len(anchor_dates)}개 사용")

    def calc_eth_per_share_multi_anchor(row_date):
        d = str(row_date)[:10]
        if d in anchors:
            return anchors[d], 0
        best_anchor_date = None
        best_dist = float("inf")
        for ad in anchor_dates:
            dist = abs((pd.Timestamp(d) - pd.Timestamp(ad)).days)
            if dist < best_dist:
                best_dist = dist
                best_anchor_date = ad
        anchor_eps = anchors[best_anchor_date]
        days_diff = (pd.Timestamp(best_anchor_date) - pd.Timestamp(d)).days
        eps = anchor_eps * (daily_factor ** days_diff)
        return eps, best_dist

    results = df["date"].apply(calc_eth_per_share_multi_anchor)
    df["eth_per_share"]      = results.apply(lambda x: x[0])
    df["backfill_quality"]   = results.apply(lambda x: x[1])

    df["eth_in_trust"]         = df["shares_outstanding"] * df["eth_per_share"]
    df["net_assets_usd"]       = df["eth_in_trust"] * (df["nav_usd"] / df["eth_per_share"])
    df["closing_price_usd"]    = df["nav_usd"]
    df["premium_discount_pct"] = 0.0
    df["basket_usd"]           = np.nan
    df["basket_eth"]           = np.nan
    df["management_fee_pct"]   = MGMT_FEE_PCT
    df["obs_ts_utc"]           = "backfill"

    print(f"[백필 완료] {len(df)}일치 데이터 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


# ── 스냅샷 저장 ───────────────────────────────────────────────────────────────
def save_snapshot(snapshot):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame([snapshot])
    if SNAPSHOT_CSV.exists():
        old_df = pd.read_csv(SNAPSHOT_CSV)
        df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        df = new_df
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df = (df.sort_values(["date", "obs_ts_utc"])
            .drop_duplicates(subset=["date"], keep="last")
            .reset_index(drop=True))
    df.to_csv(SNAPSHOT_CSV, index=False)
    return df


def merge_and_save(backfill_df, live_snap):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    live_df  = pd.DataFrame([live_snap])
    combined = pd.concat([backfill_df, live_df], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date.astype(str)
    combined = (combined.sort_values(["date", "obs_ts_utc"])
                        .drop_duplicates(subset=["date"], keep="last")
                        .reset_index(drop=True))
    combined.to_csv(SNAPSHOT_CSV, index=False)
    return combined


# ── 평단가 계산 ───────────────────────────────────────────────────────────────
def build_cost_basis_track(df, seed_avg_cost=None):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    num_cols = ["net_assets_usd", "nav_usd", "closing_price_usd", "premium_discount_pct",
                "shares_outstanding", "eth_per_share", "management_fee_pct",
                "basket_usd", "basket_eth"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)

    if "eth_in_trust" not in df.columns:
        df["eth_in_trust"] = np.nan
    df["eth_in_trust"] = pd.to_numeric(df["eth_in_trust"], errors="coerce")
    mask = df["eth_in_trust"].isna() & df["eth_per_share"].notna() & df["shares_outstanding"].notna()
    df.loc[mask, "eth_in_trust"] = df.loc[mask, "eth_per_share"] * df.loc[mask, "shares_outstanding"]

    df["implied_eth_px"] = np.where(
        (df["nav_usd"] > 0) & (df["eth_per_share"] > 0),
        df["nav_usd"] / df["eth_per_share"],
        np.nan
    )

    df["share_delta"] = df["shares_outstanding"].diff().fillna(0.0)
    df["eth_delta"]   = df["eth_in_trust"].diff().fillna(0.0)

    df["management_fee_pct"]   = df["management_fee_pct"].ffill().fillna(MGMT_FEE_PCT)
    df["premium_discount_pct"] = df["premium_discount_pct"].fillna(0.0)

    prev_eth = df["eth_in_trust"].shift(1).fillna(df["eth_in_trust"])
    day_gaps = df["date"].diff().dt.days.fillna(1).clip(lower=1)
    df["est_fee_drain_eth"] = prev_eth * (df["management_fee_pct"] / 100.0) * day_gaps / 365.0

    df["flow_eth_from_shares"]   = np.where(
        df["implied_eth_px"] > 0,
        df["share_delta"] * df["nav_usd"] / df["implied_eth_px"],
        0.0
    )
    df["flow_eth_from_holdings"] = df["eth_delta"] + df["est_fee_drain_eth"]

    flow_agreement = 1.0 - (abs(df["flow_eth_from_shares"] - df["flow_eth_from_holdings"]) /
                            (abs(df["flow_eth_from_shares"]) + abs(df["flow_eth_from_holdings"]) + 1e-12))
    df["flow_method_agreement"] = np.clip(flow_agreement, 0.0, 1.0)

    premium_abs = df["premium_discount_pct"].abs()
    w_shares    = np.clip(1.0 - premium_abs / 2.0, 0.25, 1.0)
    df["w_shares"] = w_shares
    df["flow_eth_final"] = (w_shares * df["flow_eth_from_shares"]
                            + (1.0 - w_shares) * df["flow_eth_from_holdings"])

    if not df.empty:
        df.loc[0, ["est_fee_drain_eth", "flow_eth_from_shares",
                   "flow_eth_from_holdings", "flow_eth_final"]] = 0.0

    cost_basis_usd, avg_buy_ex, eff_cost_list, inv_list, conf_list = [], [], [], [], []

    first_px = df["implied_eth_px"].dropna().iloc[0] if not df["implied_eth_px"].dropna().empty else 0.0
    seed_px  = first_px if seed_avg_cost is None else float(seed_avg_cost)

    inv = float(df.loc[0, "eth_in_trust"]) if not np.isnan(df.loc[0, "eth_in_trust"]) else 0.0
    cb  = inv * seed_px

    for i, row in df.iterrows():
        if i > 0:
            px = row["implied_eth_px"]
            if np.isnan(px):
                px = cb / inv if inv > 1e-12 else seed_px
            buy      = max(row["flow_eth_final"],  0.0)
            sell     = max(-row["flow_eth_final"], 0.0)
            prev_avg = cb / inv if inv > 1e-12 else px
            cb  += buy * px
            rm   = min(sell, inv)
            cb  -= rm * prev_avg
            inv  = max(inv + buy - rm, 0.0)

        total = float(row["eth_in_trust"]) if not np.isnan(row["eth_in_trust"]) else inv
        resid = abs(row["eth_delta"] - (row["flow_eth_final"] - row["est_fee_drain_eth"]))
        denom = max(abs(row["eth_delta"]), 1e-12)
        conf  = 0.0 if i == 0 else max(0.0, min(1.0,
                    1.0 - resid / denom - abs(row["premium_discount_pct"]) / 20.0
                    - (1.0 - row["flow_method_agreement"]) * 0.3))

        cost_basis_usd.append(cb)
        avg_buy_ex.append(cb / inv if inv > 1e-12 else np.nan)
        eff_cost_list.append(cb / total if total > 1e-12 else np.nan)
        inv_list.append(inv)
        conf_list.append(conf)

    df["flow_usd_final"]                 = df["flow_eth_final"] * df["implied_eth_px"]
    df["cumulative_flow_usd"]            = df["flow_usd_final"].cumsum()
    df["cost_basis_usd"]                 = cost_basis_usd
    df["avg_buy_price_ex_fee"]           = avg_buy_ex
    df["effective_cost_per_current_eth"] = eff_cost_list
    df["eth_inventory"]                  = inv_list
    df["confidence_score_0_1"]           = conf_list
    df["observed_annual_fee_drag_pct"]   = -df["eth_per_share"].pct_change() * (365.0 / day_gaps) * 100
    df["estimated_annual_fee_pct"]       = df["management_fee_pct"]
    df["fee_model_error_pct"]            = (df["observed_annual_fee_drag_pct"]
                                            - df["estimated_annual_fee_pct"])
    return df


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("iShares ETHB 데이터 수집 중...", flush=True)

    snapshot = build_snapshot()
    if snapshot is None:
        print("[중단] 핵심 데이터 누락으로 CSV 업데이트 스킵")
        return

    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if not existing.empty and existing["date"].iloc[-1] == snapshot["date"]:
            print(f"[스킵] 데이터 변경 없음 ({snapshot['date']}) - 재계산만 수행")
            snap_df  = save_snapshot(snapshot)
            track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)
            _save_track(track_df)
            return

    current_eth_per_share = snapshot.get("eth_per_share", np.nan)

    needs_backfill = True
    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if len(existing) >= 30:
            needs_backfill = False

    if needs_backfill and not np.isnan(current_eth_per_share):
        backfill_df = backfill_from_ishares_xls(current_eth_per_share)
        snap_df     = merge_and_save(backfill_df, snapshot)
    else:
        snap_df = save_snapshot(snapshot)

    track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)
    _save_track(track_df)
    _print_report(track_df)


def _save_track(track_df):
    out_cols = [
        "date", "eth_in_trust", "net_assets_usd", "implied_eth_px",
        "nav_usd", "closing_price_usd", "premium_discount_pct",
        "shares_outstanding", "share_delta", "eth_delta",
        "management_fee_pct", "est_fee_drain_eth", "w_shares", "flow_method_agreement", "flow_eth_final",
        "flow_usd_final", "cumulative_flow_usd",
        "eth_inventory", "cost_basis_usd",
        "avg_buy_price_ex_fee", "effective_cost_per_current_eth",
        "confidence_score_0_1",
        "observed_annual_fee_drag_pct", "estimated_annual_fee_pct", "fee_model_error_pct",
        "obs_ts_utc",
    ]
    existing_cols = [c for c in out_cols if c in track_df.columns]
    track_df[existing_cols].to_csv(TRACK_CSV, index=False)


def _print_report(track_df):
    latest   = track_df.iloc[-1]
    date_str = str(latest["date"].date())
    eth_px   = float(latest["implied_eth_px"])
    avg_buy  = float(latest["avg_buy_price_ex_fee"]) if pd.notna(latest["avg_buy_price_ex_fee"]) else 0.0
    eth_held = float(latest["eth_in_trust"]) if pd.notna(latest.get("eth_in_trust", np.nan)) else 0.0

    print("\n" + "=" * 57)
    print(f"  ETHB 평단가 추적 리포트 ({date_str})")
    print("=" * 57)
    print(f"  현재 ETH 추정 시장가     : ${eth_px:>12,.2f}")
    print(f"  펀드 순수 매수 평단가     : ${avg_buy:>12,.2f} (수수료 제외)")
    print(f"  펀드 총 ETH 보유량        : {eth_held:>14,.2f} ETH")
    print("-" * 57)

    if avg_buy > 0:
        gap_pct   = (eth_px - avg_buy) / avg_buy * 100
        gap_sign  = "+" if gap_pct >= 0 else ""
        gap_label = "프리미엄" if gap_pct >= 0 else "디스카운트"
        print(f"  현재가 vs 평단가 괴리     : {gap_sign}{gap_pct:.2f}% ({gap_label})")

    flow = float(latest["flow_eth_final"])
    flow_label = "순매수" if flow >= 0 else "순매도"
    print(f"  오늘 펀드 {flow_label}          : {abs(flow):>10,.2f} ETH")
    print(f"  [정보] 총 데이터 일수     : {len(track_df)}일")
    print("=" * 57 + "\n")


if __name__ == "__main__":
    main()
