"""Domain extraction and validation helpers."""

from __future__ import annotations

import urllib.parse


def extract_domain(url: str) -> str:
    parsed = urllib.parse.urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path).lower()
    return host.removeprefix("www.").split("/")[0]

