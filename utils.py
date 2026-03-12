# utils.py — ETF 스크래퍼 공유 유틸리티

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
