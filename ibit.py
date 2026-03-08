# ibit_avg_cost_tracker.py
# BlackRock iShares Bitcoin Trust ETF (IBIT) 펀드 BTC 평균 매수가 추적기

import re
import time
from pathlib import Path
from datetime import datetime, timezone, date as date_type

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

URL          = "https://www.ishares.com/us/products/333011/blackrock-bitcoin-etf"
XLS_URL      = ("https://www.ishares.com/us/products/333011/fund/"
                "1521942788811.ajax?fileType=xls&fileName=iShares-Bitcoin-Trust-ETF_fund&dataType=fund")
DATA_DIR     = Path("ibit_tracker")
SNAPSHOT_CSV = DATA_DIR / "ibit_daily_snapshots.csv"
TRACK_CSV    = DATA_DIR / "ibit_cost_basis_track.csv"
INCEPTION    = pd.Timestamp("2024-01-11")   # IBIT 상장일
MGMT_FEE_PCT = 0.25                          # 연 운용보수 (고정)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

SEED_AVG_COST = None   # 수동으로 알고 있으면 USD per BTC 입력


# ── 유틸 ─────────────────────────────────────────────────────────────────────
def to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).strip().replace("$", "").replace(",", "").replace("%", "")
    return float(s) if s not in {"", "-", "—"} else np.nan


def find_first(text, patterns):
    for pat in patterns:
        m = re.search(pat, text, re.I | re.S)
        if m:
            return to_float(m.group(1))
    return np.nan


# ── 오늘 데이터 스크래핑 ──────────────────────────────────────────────────────
def fetch_page_text(url=URL):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    parts = [soup.get_text(chr(10), strip=True)]
    for s in soup.find_all("script"):
        txt = s.string if s.string else s.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    return chr(10).join(parts)


def parse_snapshot(text):
    date_m = re.search(r"NAV as of\s+([A-Za-z]+ \d{1,2},\s*\d{4})", text, re.I)
    asof = (pd.to_datetime(date_m.group(1)).date().isoformat()
            if date_m else datetime.now(timezone.utc).date().isoformat())

    nav            = find_first(text, [r"NAV as of.*?\n\s*\$([\d,]+(?:\.\d+)?)"])
    net_assets     = find_first(text, [r"Net Assets of Fund\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"])
    shares         = find_first(text, [r"Shares Outstanding\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"])
    basket_usd     = find_first(text, [r"Basket Amount\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"])
    basket_btc     = find_first(text, [r"(?:Indicative )?Basket Bitcoin Amount\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"])
    closing_price  = find_first(text, [r"Closing Price\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"])
    premium_disc   = find_first(text, [r"Premium/Discount\s*\nas of.*?\n\s*([-\d.]+)"])

    btc_per_share = np.nan
    if not np.isnan(basket_btc) and not np.isnan(basket_usd) and not np.isnan(nav) and nav > 0:
        basket_shares = basket_usd / nav
        if basket_shares > 0:
            btc_per_share = basket_btc / basket_shares

    snap = {
        "date":                 asof,
        "obs_ts_utc":           datetime.now(timezone.utc).isoformat(),
        "net_assets_usd":       net_assets,
        "nav_usd":              nav,
        "closing_price_usd":    closing_price,
        "premium_discount_pct": premium_disc,
        "shares_outstanding":   shares,
        "basket_usd":           basket_usd,
        "basket_btc":           basket_btc,
        "btc_per_share":        btc_per_share,
        "management_fee_pct":   MGMT_FEE_PCT,
    }
    missing = [f for f in ["net_assets_usd", "nav_usd", "shares_outstanding", "btc_per_share"]
               if np.isnan(snap[f])]
    if missing:
        print(f"\n[경고] 웹사이트 구조 변경 의심. 파싱 실패: {', '.join(missing)}\n")
    return snap


# ── XLS 백필 (최초 1회) ───────────────────────────────────────────────────────
def backfill_from_ishares_xls(current_btc_per_share: float):
    """
    iShares 공식 XLS에서 역대 NAV + 주식수를 다운로드한 뒤,
    현재 BTC/share 값에서 역산하여 과거 BTC/share와 BTC 보유량을 계산한다.
    """
    print("[백필] iShares XLS 다운로드 중...", flush=True)
    r = requests.get(XLS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    cells = re.findall(r'<ss:Data[^>]*>([^<]+)</ss:Data>', r.text)

    # 헤더("As Of") 위치 찾기
    header_idx = next((i for i, c in enumerate(cells) if c.strip() == "As Of"), None)
    if header_idx is None:
        print("[백필 실패] XLS에서 데이터를 찾지 못했습니다.")
        return pd.DataFrame()

    # 4열씩 파싱: date, nav, ex-div(무시), shares
    rows = []
    i = header_idx + 4
    while i + 3 < len(cells):
        try:
            d    = pd.to_datetime(cells[i].strip()).date().isoformat()
            nav  = float(cells[i + 1].strip().replace(",", ""))
            shr  = float(cells[i + 3].strip().replace(",", ""))
            rows.append({"date": d, "nav_usd": nav, "shares_outstanding": shr})
            i += 4
        except Exception:
            break

    if not rows:
        print("[백필 실패] 파싱된 데이터 없음.")
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    # BTC per share 역산: 현재 → 과거 (매일 fee 차감분 역으로 더함)
    ref_date = pd.Timestamp(datetime.now(timezone.utc).date())
    daily_factor = 1.0 + (MGMT_FEE_PCT / 100.0) / 365.0

    def calc_btc_per_share(row_date):
        days_back = (ref_date - pd.Timestamp(row_date)).days
        return current_btc_per_share * (daily_factor ** days_back)

    df["btc_per_share"]  = df["date"].apply(calc_btc_per_share)
    df["btc_in_trust"]   = df["shares_outstanding"] * df["btc_per_share"]
    df["net_assets_usd"] = df["btc_in_trust"] * (df["nav_usd"] / df["btc_per_share"])  # = nav × shares
    df["closing_price_usd"]    = df["nav_usd"]   # 시장가 데이터 없음 → NAV로 대체
    df["premium_discount_pct"] = 0.0
    df["basket_usd"]           = np.nan
    df["basket_btc"]           = np.nan
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
    """백필 DataFrame + 오늘 스냅샷을 병합해서 CSV 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    live_df = pd.DataFrame([live_snap])
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
                "shares_outstanding", "btc_per_share", "management_fee_pct",
                "basket_usd", "basket_btc"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)

    # btc_in_trust 보정 (백필 행에 없을 수 있으므로)
    if "btc_in_trust" not in df.columns:
        df["btc_in_trust"] = np.nan
    df["btc_in_trust"] = pd.to_numeric(df["btc_in_trust"], errors="coerce")
    mask = df["btc_in_trust"].isna() & df["btc_per_share"].notna() & df["shares_outstanding"].notna()
    df.loc[mask, "btc_in_trust"] = df.loc[mask, "btc_per_share"] * df.loc[mask, "shares_outstanding"]

    df["implied_btc_px"] = np.where(
        (df["nav_usd"] > 0) & (df["btc_per_share"] > 0),
        df["nav_usd"] / df["btc_per_share"],
        np.nan
    )

    df["share_delta"] = df["shares_outstanding"].diff().fillna(0.0)
    df["btc_delta"]   = df["btc_in_trust"].diff().fillna(0.0)

    df["management_fee_pct"]   = df["management_fee_pct"].ffill().fillna(MGMT_FEE_PCT)
    df["premium_discount_pct"] = df["premium_discount_pct"].fillna(0.0)

    prev_btc = df["btc_in_trust"].shift(1).fillna(df["btc_in_trust"])
    day_gaps = df["date"].diff().dt.days.fillna(1).clip(lower=1)
    df["est_fee_drain_btc"] = prev_btc * (df["management_fee_pct"] / 100.0) * day_gaps / 365.0

    df["flow_btc_from_shares"]   = np.where(
        df["implied_btc_px"] > 0,
        df["share_delta"] * df["nav_usd"] / df["implied_btc_px"],
        0.0
    )
    df["flow_btc_from_holdings"] = df["btc_delta"] + df["est_fee_drain_btc"]

    premium_abs = df["premium_discount_pct"].abs()
    w_shares    = np.clip(1.0 - premium_abs / 1.0, 0.25, 1.0)
    df["flow_btc_final"] = (w_shares * df["flow_btc_from_shares"]
                            + (1.0 - w_shares) * df["flow_btc_from_holdings"])

    if not df.empty:
        df.loc[0, ["est_fee_drain_btc", "flow_btc_from_shares",
                   "flow_btc_from_holdings", "flow_btc_final"]] = 0.0

    cost_basis_usd, avg_buy_ex, eff_cost_list, inv_list, conf_list = [], [], [], [], []

    first_px = df["implied_btc_px"].dropna().iloc[0] if not df["implied_btc_px"].dropna().empty else 0.0
    seed_px  = first_px if seed_avg_cost is None else float(seed_avg_cost)

    inv = float(df.loc[0, "btc_in_trust"]) if not np.isnan(df.loc[0, "btc_in_trust"]) else 0.0
    cb  = inv * seed_px

    for i, row in df.iterrows():
        if i > 0:
            px = row["implied_btc_px"]
            if np.isnan(px):
                px = cb / inv if inv > 1e-12 else seed_px
            buy  = max(row["flow_btc_final"],  0.0)
            sell = max(-row["flow_btc_final"], 0.0)
            prev_avg = cb / inv if inv > 1e-12 else px
            cb  += buy * px
            rm   = min(sell, inv)
            cb  -= rm * prev_avg
            inv  = max(inv + buy - rm, 0.0)

        total = float(row["btc_in_trust"]) if not np.isnan(row["btc_in_trust"]) else inv
        resid = abs(row["btc_delta"] - (row["flow_btc_final"] - row["est_fee_drain_btc"]))
        denom = max(abs(row["btc_delta"]), 1e-12)
        conf  = 0.0 if i == 0 else max(0.0, min(1.0,
                    1.0 - resid / denom - abs(row["premium_discount_pct"]) / 5.0))

        cost_basis_usd.append(cb)
        avg_buy_ex.append(cb / inv if inv > 1e-12 else np.nan)
        eff_cost_list.append(cb / total if total > 1e-12 else np.nan)
        inv_list.append(inv)
        conf_list.append(conf)

    df["cost_basis_usd"]                = cost_basis_usd
    df["avg_buy_price_ex_fee"]          = avg_buy_ex
    df["effective_cost_per_current_btc"]= eff_cost_list
    df["btc_inventory"]                 = inv_list
    df["confidence_score_0_1"]          = conf_list
    df["observed_annual_fee_drag_pct"]  = -df["btc_per_share"].pct_change() * (365.0 / day_gaps) * 100
    df["estimated_annual_fee_pct"]      = df["management_fee_pct"]
    df["fee_model_error_pct"]           = (df["observed_annual_fee_drag_pct"]
                                           - df["estimated_annual_fee_pct"])
    return df


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    # 1. 오늘 데이터 스크래핑
    print("iShares IBIT 데이터 수집 중...", flush=True)
    text     = fetch_page_text()
    snapshot = parse_snapshot(text)

    # BTC per share (오늘 기준, 백필 역산 기준점)
    current_btc_per_share = snapshot.get("btc_per_share", np.nan)

    # 2. 백필 필요 여부 판단 (CSV가 없거나 30일 미만이면 자동 백필)
    needs_backfill = True
    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if len(existing) >= 30:
            needs_backfill = False

    if needs_backfill and not np.isnan(current_btc_per_share):
        backfill_df = backfill_from_ishares_xls(current_btc_per_share)
        snap_df     = merge_and_save(backfill_df, snapshot)
    else:
        snap_df = save_snapshot(snapshot)

    # 3. 평단가 계산
    track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)

    # 4. CSV 저장
    out_cols = [
        "date", "btc_in_trust", "net_assets_usd", "implied_btc_px",
        "nav_usd", "closing_price_usd", "premium_discount_pct",
        "shares_outstanding", "share_delta", "btc_delta",
        "management_fee_pct", "est_fee_drain_btc", "flow_btc_final",
        "btc_inventory", "cost_basis_usd",
        "avg_buy_price_ex_fee", "effective_cost_per_current_btc",
        "confidence_score_0_1",
        "observed_annual_fee_drag_pct", "estimated_annual_fee_pct", "fee_model_error_pct",
        "obs_ts_utc",
    ]
    existing_cols = [c for c in out_cols if c in track_df.columns]
    track_df[existing_cols].to_csv(TRACK_CSV, index=False)

    # 5. 결과 출력
    latest   = track_df.iloc[-1]
    date_str = str(latest["date"].date())
    btc_px   = float(latest["implied_btc_px"])
    avg_buy  = float(latest["avg_buy_price_ex_fee"]) if pd.notna(latest["avg_buy_price_ex_fee"]) else 0.0
    eff_cost = float(latest["effective_cost_per_current_btc"]) if pd.notna(latest["effective_cost_per_current_btc"]) else 0.0
    btc_held = float(latest["btc_in_trust"]) if pd.notna(latest.get("btc_in_trust", np.nan)) else 0.0

    print("\n" + "=" * 57)
    print(f"  IBIT 평단가 추적 리포트 ({date_str})")
    print("=" * 57)
    print(f"  현재 BTC 추정 시장가     : ${btc_px:>12,.2f}")
    print(f"  펀드 순수 매수 평단가     : ${avg_buy:>12,.2f} (수수료 제외)")
    print(f"  펀드 실질 평단가          : ${eff_cost:>12,.2f} (수수료 포함)")
    print(f"  펀드 총 BTC 보유량        : {btc_held:>14,.2f} BTC")
    print("-" * 57)

    if avg_buy > 0:
        gap_pct   = (btc_px - avg_buy) / avg_buy * 100
        gap_sign  = "+" if gap_pct >= 0 else ""
        gap_label = "프리미엄" if gap_pct >= 0 else "디스카운트"
        print(f"  현재가 vs 평단가 괴리     : {gap_sign}{gap_pct:.2f}% ({gap_label})")

    flow = float(latest["flow_btc_final"])
    flow_label = "순매수" if flow >= 0 else "순매도"
    print(f"  오늘 펀드 {flow_label}          : {abs(flow):>10,.2f} BTC")
    print(f"  오늘 BTC 보유량 변화      : {float(latest['btc_delta']):>+10,.2f} BTC")
    print(f"  오늘 수수료 소진 추정     : {-float(latest['est_fee_drain_btc']):>10,.2f} BTC")

    if len(track_df) >= 2:
        recent     = track_df.tail(min(7, len(track_df)))
        cumul_flow = float(recent["flow_btc_final"].sum())
        c_label    = "순매수" if cumul_flow >= 0 else "순매도"
        print(f"  최근 {len(recent)}일 누적 {c_label}      : {abs(cumul_flow):>10,.2f} BTC")

    print("-" * 57)
    print(f"  [정보] 추적 시작일        : {str(track_df.iloc[0]['date'].date())}")
    print(f"  [정보] 총 데이터 일수     : {len(track_df)}일")
    print(f"  [검증] 신뢰도 점수        : {float(latest['confidence_score_0_1']):.4f}")
    print("=" * 57 + "\n")


if __name__ == "__main__":
    main()
    input("[Enter 키를 눌러 종료합니다]")
