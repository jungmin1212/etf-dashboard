# utils.py — ETF 스크래퍼 공유 유틸리티

import json
import re
import time

import numpy as np
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
