# utils.py — ETF 스크래퍼 공유 유틸리티

import datetime
import json
import re
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def to_float(x):
    """문자열을 float로 변환. '$', ',', '%' 등 제거."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).strip().replace("$", "").replace(",", "").replace("%", "")
    return float(s) if s not in {"", "-", "—"} else np.nan


def find_first(text, patterns, cast="float"):
    """여러 정규식 패턴을 순서대로 시도, 첫 매치 반환."""
    flags = re.I | re.S
    for pat in patterns:
        m = re.search(pat, text, flags)
        if m:
            val = m.group(1)
            return to_float(val) if cast == "float" else val
    return np.nan if cast == "float" else None


def fetch_with_retry(url, headers=None, timeout=30, max_retries=3, backoff=5):
    """HTTP GET with retry. 실패 시 backoff 간격으로 재시도."""
    hdrs = headers or HEADERS
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=hdrs, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = backoff * (attempt + 1)
            print(f"[재시도 {attempt+1}/{max_retries}] {e} - {wait}초 후 재시도")
            time.sleep(wait)


def fetch_page_text(url, headers=None):
    """URL에서 HTML을 가져와 텍스트+스크립트 내용 추출."""
    r = fetch_with_retry(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    parts = [soup.get_text(chr(10), strip=True)]
    for s in soup.find_all("script"):
        txt = s.string if s.string else s.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    return chr(10).join(parts)


def _extract_balanced_json(s, brace_idx):
    """s[brace_idx]가 '{' 라고 가정하고, 균형 잡힌 JSON 객체 문자열을 잘라서 반환."""
    depth = 0
    in_str = False
    esc = False
    for i in range(brace_idx, len(s)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return s[brace_idx:i + 1]
    return None


def fetch_ishares_datapoints(url, headers=None):
    """
    iShares 신규(Astro 기반) 상품 페이지에 임베드된 데이터 포인트 JSON을 추출.

    페이지 마크업이 HTML 엔티티로 인코딩된 JSON 블롭(&quot; 등)을 포함하고 있어
    가시 텍스트 정규식이 아니라 이 JSON을 직접 파싱하는 편이 훨씬 안정적이다.
    "dataPoints"(키 지표 카드)와 fundHeader.fundNav의 "dataPointsByNameMap"
    (NAV 등, raw value 포함) 두 블록을 병합해 {name: {formattedValue, value, formattedAsOfDate}} 로 반환.
    """
    r = fetch_with_retry(url, headers=headers)
    html = (r.text.replace("&quot;", '"')
                  .replace("&#x27;", "'")
                  .replace("&amp;", "&"))

    points = {}

    dp_idx = html.find('"dataPoints":{')
    if dp_idx != -1:
        block = _extract_balanced_json(html, dp_idx + len('"dataPoints":'))
        if block:
            try:
                obj = json.loads(block)
                for key, v in obj.items():
                    if isinstance(v, dict) and "formattedValue" in v:
                        points[key] = v
            except json.JSONDecodeError:
                pass

    nav_idx = html.find('"fundNav":{')
    if nav_idx != -1:
        map_idx = html.find('"dataPointsByNameMap":{', nav_idx)
        if map_idx != -1:
            block = _extract_balanced_json(html, map_idx + len('"dataPointsByNameMap":'))
            if block:
                try:
                    obj = json.loads(block)
                    for key, v in obj.items():
                        if isinstance(v, dict) and "formattedValue" in v:
                            points[key] = v
                except json.JSONDecodeError:
                    pass

    return points


def dp_float(points, key):
    """fetch_ishares_datapoints() 결과에서 key 값을 float로 추출 (raw value 우선)."""
    entry = points.get(key)
    if not entry:
        return np.nan
    v = entry.get("value")
    if isinstance(v, (int, float)):
        return float(v)
    return to_float(entry.get("formattedValue"))


# ── Farside Investors 일별 순유입 데이터 (수집 공백 메우기용) ──────────────────
FARSIDE_URLS = {
    "bitcoin":   "https://farside.co.uk/bitcoin-etf-flow-all-data/",
    "ethereum":  "https://farside.co.uk/ethereum-etf-flow-all-data/",
}
# 각 자산 테이블에서 "Date" 다음 티커 컬럼의 순서상 인덱스(0-base, td 기준)
FARSIDE_TICKER_COL = {
    ("bitcoin", "IBIT"): 1,
    ("ethereum", "ETHA"): 1,
    ("ethereum", "ETHB"): 2,
}

_DATE_RE = re.compile(r"^\d{1,2} \w{3} \d{4}$")


def _farside_cell_to_float(text):
    """Farside 테이블 셀 텍스트('116.5', '(45.4)', '0.0', '-')를 $백만 단위 float로."""
    t = text.strip().replace(",", "")
    if t in {"", "-"}:
        return np.nan
    neg = t.startswith("(") and t.endswith(")")
    if neg:
        t = t[1:-1]
    try:
        v = float(t)
    except ValueError:
        return np.nan
    return -v if neg else v


def fetch_farside_flows(asset, ticker, headers=None):
    """
    Farside Investors의 ETF 일별 순유입(자금 흐름, $백만) 전체 이력을 가져온다.
    asset: "bitcoin" 또는 "ethereum". ticker: "IBIT"/"ETHA"/"ETHB".
    반환: {date(YYYY-MM-DD): flow_musd(float, 상장 전/데이터 없음은 NaN)}
    """
    url = FARSIDE_URLS[asset]
    col_idx = FARSIDE_TICKER_COL[(asset, ticker)]
    r = fetch_with_retry(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if table is None:
        return {}

    out = {}
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        date_text = cells[0].get_text(strip=True)
        if not _DATE_RE.match(date_text):
            continue
        if len(cells) <= col_idx:
            continue
        d = datetime.datetime.strptime(date_text, "%d %b %Y").date().isoformat()
        out[d] = _farside_cell_to_float(cells[col_idx].get_text(strip=True))
    return out


def fetch_daily_prices(yf_symbol, start_date, end_date):
    """yfinance에서 start_date~end_date(포함) 일별 종가를 {date: price} 로 반환."""
    import yfinance as yf
    end_exclusive = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat()
    hist = yf.Ticker(yf_symbol).history(start=str(start_date), end=end_exclusive)
    return {ts.date().isoformat(): float(px) for ts, px in hist["Close"].items()}


def backfill_gap_with_farside(existing_df, new_snapshot, asset, ticker,
                               coin_per_share_col, basket_col, yf_symbol, mgmt_fee_pct, headers=None):
    """
    직전 실측 스냅샷과 오늘 스냅샷 사이에 수집 공백(여러 날)이 있을 때,
    Farside Investors의 실측 일별 순유입($) + 일별 시세로 그 사이를 근사 복원한다.

    각 날짜의 방향/크기는 Farside 실측치를 그대로 따르되, 두 실측 앵커(공백 시작/끝의
    실제 iShares 보유량)에 정확히 맞도록 누적 흐름을 비례 보정(reconcile)한다.
    공백이 없거나(1일 이하) Farside 데이터를 하나도 못 가져오면 None 반환.
    """
    if existing_df.empty:
        return None

    last_row = existing_df.iloc[-1]
    last_date = pd.to_datetime(last_row["date"]).date()
    new_date = pd.to_datetime(new_snapshot["date"]).date()
    gap_days = (new_date - last_date).days
    if gap_days <= 1:
        return None

    start_cps = to_float(last_row.get(coin_per_share_col))
    end_cps = to_float(new_snapshot.get(coin_per_share_col))
    start_shares = to_float(last_row.get("shares_outstanding"))
    end_shares = to_float(new_snapshot.get("shares_outstanding"))
    if any(np.isnan(x) for x in (start_cps, end_cps, start_shares, end_shares)):
        return None
    start_total = start_cps * start_shares
    end_total = end_cps * end_shares

    try:
        flows = fetch_farside_flows(asset, ticker, headers=headers)
        prices = fetch_daily_prices(yf_symbol, last_date, new_date)
    except Exception as e:
        print(f"[Farside 백필 실패] {e}")
        return None

    missing_dates = [
        (last_date + datetime.timedelta(days=i)).isoformat()
        for i in range(1, gap_days)
    ]
    steps = []  # (date, flow_coin)
    for d in missing_dates:
        flow_musd = flows.get(d)
        price = prices.get(d)
        if flow_musd is None or np.isnan(flow_musd) or price is None:
            continue
        steps.append((d, flow_musd * 1_000_000 / price))

    if not steps:
        return None

    raw_end = start_total + sum(s[1] for s in steps)
    denom = raw_end - start_total
    scale = (end_total - start_total) / denom if abs(denom) > 1e-9 else 1.0

    n = len(steps)
    rows = []
    running_total = start_total
    for i, (d, flow_coin) in enumerate(steps):
        running_total += flow_coin * scale
        w = (i + 1) / n
        cps = start_cps + (end_cps - start_cps) * w
        shares = running_total / cps
        price = prices[d]
        nav = cps * price  # 펀드 주당 NAV (기초자산 시세가 아님)
        row = {
            "date": d,
            "obs_ts_utc": "farside_backfill",
            "net_assets_usd": nav * shares,
            "nav_usd": nav,
            "closing_price_usd": nav,  # 프리미엄/디스카운트 0 가정이므로 NAV와 동일하게 근사
            "premium_discount_pct": 0.0,
            "shares_outstanding": shares,
            "basket_usd": np.nan,
            "management_fee_pct": mgmt_fee_pct,
            coin_per_share_col: cps,
            basket_col: np.nan,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    print(f"[Farside 백필] {ticker}: {last_date} ~ {new_date} 공백 {len(df)}일 근사 복원 "
          f"(스케일 보정 {scale:.3f})")
    return df
