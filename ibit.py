# ibit_avg_cost_tracker.py
# BlackRock iShares Bitcoin Trust ETF (IBIT) íë BTC íê·  ë§¤ìê° ì¶ì ê¸°

import re
from pathlib import Path
from datetime import datetime, timezone, date as date_type

import numpy as np
import pandas as pd

from utils import to_float, find_first, fetch_with_retry, fetch_page_text, HEADERS

URL          = "https://www.ishares.com/us/products/333011/blackrock-bitcoin-etf"
XLS_URL      = ("https://www.ishares.com/us/products/333011/fund/"
                "1521942788811.ajax?fileType=xls&fileName=iShares-Bitcoin-Trust-ETF_fund&dataType=fund")
DATA_DIR     = Path("ibit_tracker")
SNAPSHOT_CSV = DATA_DIR / "ibit_daily_snapshots.csv"
TRACK_CSV    = DATA_DIR / "ibit_cost_basis_track.csv"
INCEPTION    = pd.Timestamp("2024-01-11")   # IBIT ìì¥ì¼
MGMT_FEE_PCT = 0.25                          # ì° ì´ì©ë³´ì (ê³ ì )

SEED_AVG_COST = None   # ìëì¼ë¡ ìê³  ìì¼ë©´ USD per BTC ìë ¥


# ââ XLSìì ìµì  NAV/ì£¼ìì ê°ì ¸ì¤ê¸° (Primary ìì¤) ââââââââââââââââââââââââââââ
def fetch_xls_latest():
    """
    iShares ê³µì XLSìì ìµì  íì date, nav_usd, shares_outstanding ì¶ì¶.
    ì¤í¨ ì None ë°í.
    """
    try:
        print("[XLS] iShares XLS ë¤ì´ë¡ë ì¤...", flush=True)
        r = fetch_with_retry(XLS_URL, headers=HEADERS, timeout=30)
        cells = re.findall(r'<ss:Data[^>]*>([^<]+)</ss:Data>', r.text)

        header_idx = next((i for i, c in enumerate(cells) if c.strip() == "As Of"), None)
        if header_idx is None:
            print("[XLS] í¤ëë¥¼ ì°¾ì§ ëª»í¨ â HTML fallback")
            return None

        # 4ì´ì© íì±: date, nav, ex-div(ë¬´ì), shares â ë§ì§ë§ ì í¨ íì´ ìµì 
        # XLSë ìµì âê³¼ê±° ì ì ë ¬ â ëª¨ë  í íì± í ìµë ë ì§ ì í
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
        print(f"[XLS] ìµì  ë°ì´í°: {latest['date']} | NAV=${latest['nav_usd']:.2f} | ì£¼ìì={latest['shares_outstanding']:,.0f}")
        return latest
    except Exception as e:
        print(f"[XLS ì¤í¨] {e} â HTML fallback ì¬ì©")
        return None


# ââ HTMLìì basket/closing/premium ê°ì ¸ì¤ê¸° ââââââââââââââââââââââââââââââââââ
def fetch_html_supplementary():
    """HTML íì´ì§ìì XLSì ìë íë(basket_btc, closing_price, premium_discount) ì¶ì¶."""
    text = fetch_page_text(URL, headers=HEADERS)

    date_m = re.search(r"NAV as of\s+([A-Za-z]+ \d{1,2},\s*\d{4})", text, re.I)
    asof = (pd.to_datetime(date_m.group(1)).date().isoformat()
            if date_m else datetime.now(timezone.utc).date().isoformat())

    data = {
        "date":                 asof,
        "net_assets_usd":       find_first(text, [r"Net Assets of Fund\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "basket_usd":           find_first(text, [r"Basket Amount\s*\nas of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "basket_btc":           find_first(text, [r"(?:Indicative )?Basket Bitcoin Amount\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"]),
        "closing_price_usd":    find_first(text, [r"Closing Price\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"]),
        "premium_discount_pct": find_first(text, [r"Premium/Discount\s*\nas of.*?\n\s*([-\d.]+)"]),
        # HTML fallbackì© (XLS ì¤í¨ ì)
        "nav_usd":              find_first(text, [r"NAV as of.*?\n\s*\$([\d,]+(?:\.\d+)?)"]),
        "shares_outstanding":   find_first(text, [r"Shares Outstanding\s*\nas of.*?\n\s*([\d,]+(?:\.\d+)?)"]),
    }
    return data


# ââ ì¤ëì· ì¡°ë¦½ ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def build_snapshot():
    """XLS(primary) + HTML(supplementary)ë¡ ì¤ë ì¤ëì· ìì±."""
    xls_data = fetch_xls_latest()
    html_data = fetch_html_supplementary()

    # XLSê° primary â NAV, shares_outstanding ì°ì  ì¬ì©
    if xls_data:
        nav    = xls_data["nav_usd"]
        shares = xls_data["shares_outstanding"]
        asof   = xls_data["date"]
    else:
        nav    = html_data["nav_usd"]
        shares = html_data["shares_outstanding"]
        asof   = html_data["date"]

    basket_usd = html_data["basket_usd"]
    basket_btc = html_data["basket_btc"]

    btc_per_share = np.nan
    if not np.isnan(basket_btc) and not np.isnan(basket_usd) and not np.isnan(nav) and nav > 0:
        basket_shares = basket_usd / nav
        if basket_shares > 0:
            btc_per_share = basket_btc / basket_shares

    snap = {
        "date":                 asof,
        "obs_ts_utc":           datetime.now(timezone.utc).isoformat(),
        "net_assets_usd":       html_data["net_assets_usd"],
        "nav_usd":              nav,
        "closing_price_usd":    html_data["closing_price_usd"],
        "premium_discount_pct": html_data["premium_discount_pct"],
        "shares_outstanding":   shares,
        "basket_usd":           basket_usd,
        "basket_btc":           basket_btc,
        "btc_per_share":        btc_per_share,
        "management_fee_pct":   MGMT_FEE_PCT,
    }

    # ë¬´ê²°ì± ê²ì¦
    critical = ["nav_usd", "shares_outstanding", "btc_per_share"]
    missing = [f for f in critical if np.isnan(snap[f])]
    if missing:
        print(f"\n[ê²½ê³ ] íµì¬ ë°ì´í° ëë½: {', '.join(missing)}")
        print("  ì¹ì¬ì´í¸ êµ¬ì¡° ë³ê²½ ìì¬ â CSV ìë°ì´í¸ ì¤íµ\n")
        return None

    return snap


# ââ XLS ë°±í (ìµì´ 1í) âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def backfill_from_ishares_xls(current_btc_per_share: float):
    """
    iShares ê³µì XLSìì ì­ë NAV + ì£¼ììë¥¼ ë¤ì´ë¡ëí ë¤,
    íì¬ BTC/share ê°ìì ì­ì°íì¬ ê³¼ê±° BTC/shareì BTC ë³´ì ëì ê³ì°íë¤.
    """
    print("[ë°±í] iShares XLS ë¤ì´ë¡ë ì¤...", flush=True)
    r = fetch_with_retry(XLS_URL, headers=HEADERS, timeout=30)
    cells = re.findall(r'<ss:Data[^>]*>([^<]+)</ss:Data>', r.text)

    header_idx = next((i for i, c in enumerate(cells) if c.strip() == "As Of"), None)
    if header_idx is None:
        print("[ë°±í ì¤í¨] XLSìì ë°ì´í°ë¥¼ ì°¾ì§ ëª»íìµëë¤.")
        return pd.DataFrame()

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
        print("[ë°±í ì¤í¨] íì±ë ë°ì´í° ìì.")
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    ref_date = pd.Timestamp(datetime.now(timezone.utc).date())
    daily_factor = 1.0 + (MGMT_FEE_PCT / 100.0) / 365.0

    def calc_btc_per_share(row_date):
        days_back = (ref_date - pd.Timestamp(row_date)).days
        return current_btc_per_share * (daily_factor ** days_back)

    df["btc_per_share"]  = df["date"].apply(calc_btc_per_share)
    df["btc_in_trust"]   = df["shares_outstanding"] * df["btc_per_share"]
    df["net_assets_usd"] = df["btc_in_trust"] * (df["nav_usd"] / df["btc_per_share"])
    df["closing_price_usd"]    = df["nav_usd"]
    df["premium_discount_pct"] = 0.0
    df["basket_usd"]           = np.nan
    df["basket_btc"]           = np.nan
    df["management_fee_pct"]   = MGMT_FEE_PCT
    df["obs_ts_utc"]           = "backfill"

    print(f"[ë°±í ìë£] {len(df)}ì¼ì¹ ë°ì´í° ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


# ââ ì¤ëì· ì ì¥ âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
    """ë°±í DataFrame + ì¤ë ì¤ëì·ì ë³í©í´ì CSV ì ì¥"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    live_df = pd.DataFrame([live_snap])
    combined = pd.concat([backfill_df, live_df], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date.astype(str)
    combined = (combined.sort_values(["date", "obs_ts_utc"])
                        .drop_duplicates(subset=["date"], keep="last")
                        .reset_index(drop=True))
    combined.to_csv(SNAPSHOT_CSV, index=False)
    return combined


# ââ íë¨ê° ê³ì° âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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


# ââ main ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def main():
    print("iShares IBIT ë°ì´í° ìì§ ì¤...", flush=True)

    # 1. ì¤ë ì¤ëì· ìì± (XLS primary + HTML supplementary)
    snapshot = build_snapshot()
    if snapshot is None:
        print("[ì¤ë¨] íµì¬ ë°ì´í° ëë½ì¼ë¡ CSV ìë°ì´í¸ ì¤íµ")
        return

    # 2. ì¤ë§í¸ ì¤íµ: ê°ì ë ì§ ë°ì´í°ê° ì´ë¯¸ ìì¼ë©´ track ì¬ê³ì°ë§
    if SNAPSHOT_CSV.exists():
        existing = pd.read_csv(SNAPSHOT_CSV)
        if not existing.empty and existing["date"].iloc[-1] == snapshot["date"]:
            print(f"[ì¤íµ] ë°ì´í° ë³ê²½ ìì ({snapshot['date']}) - ì¬ê³ì°ë§ ìí")
            # obs_ts_utcë§ ìë°ì´í¸ (ê°ì ë ì§ì§ë§ ìµì  ê´ì¸¡)
            snap_df = save_snapshot(snapshot)
            track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)
            _save_track(track_df)
            return

    # 3. BTC per share (ë°±í ì­ì° ê¸°ì¤ì )
    current_btc_per_share = snapshot.get("btc_per_share", np.nan)

    # 4. ë°±í íì ì¬ë¶ íë¨ (CSVê° ìê±°ë 30ì¼ ë¯¸ë§ì´ë©´ ìë ë°±í)
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

    # 5. íë¨ê° ê³ì°
    track_df = build_cost_basis_track(snap_df, seed_avg_cost=SEED_AVG_COST)

    # 6. CSV ì ì¥ + ê²°ê³¼ ì¶ë ¥
    _save_track(track_df)
    _print_report(track_df)


def _save_track(track_df):
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


def _print_report(track_df):
    latest   = track_df.iloc[-1]
    date_str = str(latest["date"].date())
    btc_px   = float(latest["implied_btc_px"])
    avg_buy  = float(latest["avg_buy_price_ex_fee"]) if pd.notna(latest["avg_buy_price_ex_fee"]) else 0.0
    eff_cost = float(latest["effective_cost_per_current_btc"]) if pd.notna(latest["effective_cost_per_current_btc"]) else 0.0
    btc_held = float(latest["btc_in_trust"]) if pd.notna(latest.get("btc_in_trust", np.nan)) else 0.0

    print("\n" + "=" * 57)
    print(f"  IBIT íë¨ê° ì¶ì  ë¦¬í¬í¸ ({date_str})")
    print("=" * 57)
    print(f"  íì¬ BTC ì¶ì  ìì¥ê°     : ${btc_px:>12,.2f}")
    print(f"  íë ìì ë§¤ì íë¨ê°     : ${avg_buy:>12,.2f} (ììë£ ì ì¸)")
    print(f"  íë ì¤ì§ íë¨ê°          : ${eff_cost:>12,.2f} (ììë£ í¬í¨)")
    print(f"  íë ì´ BTC ë³´ì ë        : {btc_held:>14,.2f} BTC")
    print("-" * 57)

    if avg_buy > 0:
        gap_pct   = (btc_px - avg_buy) / avg_buy * 100
        gap_sign  = "+" if gap_pct >= 0 else ""
        gap_label = "íë¦¬ë¯¸ì" if gap_pct >= 0 else "ëì¤ì¹´ì´í¸"
        print(f"  íì¬ê° vs íë¨ê° ê´´ë¦¬     : {gap_sign}{gap_pct:.2f}% ({gap_label})")

    flow = float(latest["flow_btc_final"])
    flow_label = "ìë§¤ì" if flow >= 0 else "ìë§¤ë"
    print(f"  ì¤ë íë {flow_label}          : {abs(flow):>10,.2f} BTC")
    print(f"  ì¤ë BTC ë³´ì ë ë³í      : {float(latest['btc_delta']):>+10,.2f} BTC")
    print(f"  ì¤ë ììë£ ìì§ ì¶ì      : {-float(latest['est_fee_drain_btc']):>10,.2f} BTC")

    if len(track_df) >= 2:
        recent     = track_df.tail(min(7, len(track_df)))
        cumul_flow = float(recent["flow_btc_final"].sum())
        c_label    = "ìë§¤ì" if cumul_flow >= 0 else "ìë§¤ë"
        print(f"  ìµê·¼ {len(recent)}ì¼ ëì  {c_label}      : {abs(cumul_flow):>10,.2f} BTC")

    print("-" * 57)
    print(f"  [ì ë³´] ì¶ì  ììì¼        : {str(track_df.iloc[0]['date'].date())}")
    print(f"  [ì ë³´] ì´ ë°ì´í° ì¼ì     : {len(track_df)}ì¼")
    print(f"  [ê²ì¦] ì ë¢°ë ì ì        : {float(latest['confidence_score_0_1']):.4f}")
    print("=" * 57 + "\n")


if __name__ == "__main__":
    main()
