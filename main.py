#!/usr/bin/env python3
"""
IPTV 自动优选 — 两阶段测速版
阶段一: HTTP 快速连通 + 速度测试，筛出每频道候选
阶段二: ffprobe 精测分辨率/码率/编码，按清晰度+速度综合排序
"""

import json
import os
import re
import shutil
import subprocess
import time
import traceback
from collections import OrderedDict, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====================== 路径 ======================
ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "output"

SOURCES_FILE = CONFIG_DIR / "sources.txt"
ALIAS_FILE = CONFIG_DIR / "alias.txt"
ALLOW_LIST_FILE = CONFIG_DIR / "allow_list.txt"
BLACKLIST_FILE = CONFIG_DIR / "blacklist.txt"
TEMPLATE_FILE = CONFIG_DIR / "template_output.txt"
HISTORY_FILE = CONFIG_DIR / "history.json"

OUTPUT_M3U = OUTPUT_DIR / "iptv.m3u"
OUTPUT_TV = OUTPUT_DIR / "tv.txt"
REPORT_JSON = OUTPUT_DIR / "report.json"

# ====================== 通用参数 ======================
SOURCE_DOWNLOAD_WORKERS = int(os.getenv("SOURCE_DOWNLOAD_WORKERS", "6"))
STREAM_TEST_WORKERS = int(os.getenv("STREAM_TEST_WORKERS", "10"))
KEEP_PER_CHANNEL = int(os.getenv("KEEP_PER_CHANNEL", "5"))
PRETEST_MAX_PER_CHANNEL = int(os.getenv("PRETEST_MAX_PER_CHANNEL", "80"))
MAX_TOTAL_TEST_URLS = int(os.getenv("MAX_TOTAL_TEST_URLS", "3000"))

CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "3"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "6"))
SOURCE_STAGE_MAX_SECONDS = float(os.getenv("SOURCE_STAGE_MAX_SECONDS", "180"))
TEST_STAGE_MAX_SECONDS = float(os.getenv("TEST_STAGE_MAX_SECONDS", "1800"))
SINGLE_URL_MAX_SECONDS = float(os.getenv("SINGLE_URL_MAX_SECONDS", "12"))
SOURCE_MAX_BYTES = int(os.getenv("SOURCE_MAX_BYTES", str(3 * 1024 * 1024)))
DIRECT_READ_BYTES = int(os.getenv("DIRECT_READ_BYTES", str(512 * 1024)))
SEGMENT_READ_BYTES = int(os.getenv("SEGMENT_READ_BYTES", str(256 * 1024)))
MIN_VALID_BYTES = int(os.getenv("MIN_VALID_BYTES", str(16 * 1024)))
M3U8_SEGMENT_TEST_COUNT = int(os.getenv("M3U8_SEGMENT_TEST_COUNT", "1"))

# ====================== 质量检测参数 ======================
# ffprobe 精测：对每频道 HTTP 测速前 N 名做 ffprobe，获取真实分辨率/码率/编码
FFPROBE_ENABLED = os.getenv("FFPROBE_ENABLED", "1") not in ("0", "false", "no", "")
FFPROBE_PATH = shutil.which("ffprobe") or ""
FFPROBE_TOP_N = int(os.getenv("FFPROBE_TOP_N", "5"))
FFPROBE_TIMEOUT = int(os.getenv("FFPROBE_TIMEOUT", "8"))

# 持续稳定性测试：对最终候选做 N 秒持续拉流，检测是否断流（0=关闭）
SUSTAINED_TEST_SECONDS = float(os.getenv("SUSTAINED_TEST_SECONDS", "0"))
SUSTAINED_TEST_TOP_N = int(os.getenv("SUSTAINED_TEST_TOP_N", "3"))

# ====================== 分辨率过滤 ======================
# 低于此分辨率的源直接丢弃（0=不过滤）。720 = 丢弃 720P 以下
MIN_HEIGHT = int(os.getenv("MIN_HEIGHT", "720"))

# ====================== 评分权重（速度优先） ======================
W_AVAILABILITY = float(os.getenv("W_AVAILABILITY", "0.25"))
W_QUALITY = float(os.getenv("W_QUALITY", "0.20"))
W_SPEED = float(os.getenv("W_SPEED", "0.35"))
W_LATENCY = float(os.getenv("W_LATENCY", "0.10"))
W_STABILITY = float(os.getenv("W_STABILITY", "0.10"))

DEFAULT_HEADERS = {
    "User-Agent": os.getenv(
        "IPTV_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    ),
    "Accept": "*/*",
    "Connection": "close",
}

URL_SUFFIX_RE = re.compile(r"\$.*$")
M3U_GROUP_RE = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)

# m3u8 master playlist 变体流属性
STREAM_INF_RE = re.compile(r"#EXT-X-STREAM-INF:([^\n]*)", re.IGNORECASE)
BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)", re.IGNORECASE)
RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)", re.IGNORECASE)
CODECS_RE = re.compile(r'CODECS="([^"]*)"', re.IGNORECASE)


# ====================== 工具函数 ======================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dirs() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="gb18030", errors="ignore")
    except Exception as exc:
        print(f"【读取失败】{path.name}: {str(exc)[:100]}")
        return ""


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def atomic_write_json(path: Path, data: dict) -> None:
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))


def clean_url(raw_url: str) -> str:
    return URL_SUFFIX_RE.sub("", raw_url.strip())


def unique_keep_order(items: List[str]) -> List[str]:
    return list(dict.fromkeys([x for x in items if x]))


def load_lines(path: Path) -> List[str]:
    lines = []
    for raw in safe_read_text(path).splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines


def host_of(url: str) -> str:
    return urlparse(url).netloc.lower()


# ====================== 配置加载 ======================
def load_alias() -> Dict[str, str]:
    alias_map = {}
    for raw in load_lines(ALIAS_FILE):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if not parts:
            continue
        std_name = parts[0]
        for name in parts:
            alias_map[name] = std_name
    print(f"【配置】别名映射 {len(alias_map)} 条")
    return alias_map


def load_blacklist() -> Tuple[set, List[str], List[str]]:
    exact_channels = set()
    fuzzy_channels = []
    url_keywords = []
    for raw in load_lines(BLACKLIST_FILE):
        if raw.startswith("url*"):
            keyword = raw[4:].strip()
            if keyword:
                url_keywords.append(keyword)
        elif raw.startswith("*"):
            keyword = raw[1:].strip()
            if keyword:
                fuzzy_channels.append(keyword)
        else:
            exact_channels.add(raw)
    print(
        f"【配置】频道精确黑名单 {len(exact_channels)} 条，"
        f"频道模糊黑名单 {len(fuzzy_channels)} 条，URL黑名单 {len(url_keywords)} 条"
    )
    return exact_channels, fuzzy_channels, url_keywords


def is_black_channel(name: str, exact_channels: set, fuzzy_channels: List[str]) -> bool:
    return name in exact_channels or any(keyword in name for keyword in fuzzy_channels)


def is_black_url(url: str, url_keywords: List[str]) -> bool:
    return any(keyword in url for keyword in url_keywords)


def load_template() -> Tuple[List[str], Dict[str, Tuple[str, str]]]:
    order = []
    info = {}
    for raw in load_lines(TEMPLATE_FILE):
        if "#genre#" in raw:
            continue
        parts = [p.strip() for p in raw.split("|")]
        std_name = parts[0] if parts else ""
        if not std_name:
            continue
        display_name = parts[1] if len(parts) >= 2 and parts[1] else std_name
        group_name = parts[2] if len(parts) >= 3 and parts[2] else "默认分组"
        order.append(std_name)
        info[std_name] = (display_name, group_name)
    print(f"【配置】输出模板频道 {len(order)} 个")
    return order, info


def load_history() -> dict:
    text = safe_read_text(HISTORY_FILE)
    if not text:
        return {"version": 2, "urls": {}}
    try:
        data = json.loads(text)
        if "urls" not in data:
            data["urls"] = {}
        return data
    except Exception:
        print("【警告】history.json 解析失败，将重新生成历史记录")
        return {"version": 2, "urls": {}}


def read_sources() -> List[str]:
    sources = load_lines(SOURCES_FILE)
    print(f"【配置】订阅源 {len(sources)} 个")
    return sources


# ====================== 源下载 ======================
def download_source(url: str) -> Tuple[str, str]:
    start = time.perf_counter()
    try:
        with requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            verify=False,
            stream=True,
        ) as resp:
            if not (200 <= resp.status_code < 400):
                return url, ""
            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= SOURCE_MAX_BYTES:
                    print(f"【源截断】{url} 超过 {SOURCE_MAX_BYTES // 1024}KB")
                    break
                if time.perf_counter() - start >= SINGLE_URL_MAX_SECONDS:
                    print(f"【源超时截断】{url} 下载超过 {SINGLE_URL_MAX_SECONDS:.0f} 秒")
                    break
            return url, b"".join(chunks).decode(resp.encoding or "utf-8", errors="ignore")
    except Exception as exc:
        print(f"【源下载失败】{url} | {str(exc)[:100]}")
        return url, ""


# ====================== 解析 ======================
def parse_m3u(content: str) -> Dict[str, List[str]]:
    result = defaultdict(list)
    current_name = ""
    for raw in content.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            current_name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
        elif line.startswith(("http://", "https://")) and current_name:
            result[current_name].append(clean_url(line))
    return result


def parse_txt(content: str) -> Dict[str, List[str]]:
    result = defaultdict(list)
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "#genre#" in line:
            continue
        line = re.split(r"\s+#", line, maxsplit=1)[0].strip()
        if "," not in line:
            continue
        name, url = [x.strip() for x in line.split(",", 1)]
        if name and url.startswith(("http://", "https://")):
            result[name].append(clean_url(url))
    return result


def parse_source(content: str) -> Dict[str, List[str]]:
    if "#EXTM3U" in content[:1024] or "#EXTINF" in content:
        return parse_m3u(content)
    return parse_txt(content)


def parse_m3u8_variants(base_url: str, text: str) -> List[dict]:
    """
    解析 m3u8 master playlist 中的所有变体流。
    返回按 BANDWIDTH 降序排列的变体列表，每个变体包含:
    url, bandwidth, width, height, codecs
    """
    variants = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        # 下一行就是变体 URL
        variant_url = ""
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if next_line and not next_line.startswith("#"):
                variant_url = urljoin(base_url, next_line)

        bandwidth = 0
        width = 0
        height = 0
        codecs = ""

        bw_match = BANDWIDTH_RE.search(line)
        if bw_match:
            bandwidth = int(bw_match.group(1))

        res_match = RESOLUTION_RE.search(line)
        if res_match:
            width = int(res_match.group(1))
            height = int(res_match.group(2))

        codec_match = CODECS_RE.search(line)
        if codec_match:
            codecs = codec_match.group(1)

        if variant_url:
            variants.append({
                "url": variant_url,
                "bandwidth": bandwidth,
                "width": width,
                "height": height,
                "codecs": codecs,
            })

    variants.sort(key=lambda v: v["bandwidth"], reverse=True)
    return variants


def extract_m3u8_links(base_url: str, text: str) -> Tuple[List[str], List[str]]:
    child_playlists = []
    segments = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        absolute = urljoin(base_url, line)
        if ".m3u8" in urlparse(absolute).path.lower():
            child_playlists.append(absolute)
        else:
            segments.append(absolute)
    return child_playlists, segments


# ====================== 内容格式检测 ======================
def detect_content_format(content_type: str, first_bytes: bytes) -> str:
    """
    通过 Content-Type 和首字节魔法值判断流格式。
    返回: 'ts', 'flv', 'mp4', 'm3u8', 'html', 'unknown'
    """
    ct = content_type.lower()

    if b"#EXTM3U" in first_bytes[:64]:
        return "m3u8"
    # TS 流同步字节 0x47
    if len(first_bytes) > 0 and first_bytes[0] == 0x47:
        return "ts"
    # FLV 头
    if first_bytes[:3] == b"FLV":
        return "flv"
    # MP4 ftyp box
    if b"ftyp" in first_bytes[:16]:
        return "mp4"

    if "mpegurl" in ct:
        return "m3u8"
    if "mp2t" in ct or "mpegts" in ct:
        return "ts"
    if "flv" in ct:
        return "flv"
    if "mp4" in ct:
        return "mp4"
    if "text/html" in ct:
        return "html"
    return "unknown"


def format_is_video(fmt: str) -> bool:
    return fmt in ("ts", "flv", "mp4", "m3u8")


# ====================== HTTP 测速（阶段一） ======================
def read_limited_response(
    resp: requests.Response,
    limit_bytes: int,
    start: float,
    max_seconds: float,
) -> Tuple[int, float, float, bytes]:
    """
    读取有限响应，返回 (总字节数, 连接耗时, 下载耗时, 首块字节)。
    连接耗时 = 从开始到收到首字节
    下载耗时 = 从首字节到读取完毕
    """
    total = 0
    first_byte_time = None
    first_chunk = b""
    for chunk in resp.iter_content(chunk_size=64 * 1024):
        if not chunk:
            break
        if first_byte_time is None:
            first_byte_time = time.perf_counter()
            first_chunk = chunk[:256]
        total += len(chunk)
        if total >= limit_bytes:
            break
        if time.perf_counter() - start >= max_seconds:
            break

    now = time.perf_counter()
    if first_byte_time is not None:
        connect_time = first_byte_time - start
        download_time = now - first_byte_time
    else:
        connect_time = now - start
        download_time = 0.0

    return total, connect_time, download_time, first_chunk


def fetch_text_limited(
    url: str, limit_bytes: int = 256 * 1024
) -> Tuple[bool, str, str, int, float, float]:
    """
    获取文本内容（m3u8 playlist），返回:
    (ok, text, content_type, bytes, connect_time, download_time)
    """
    start = time.perf_counter()
    try:
        with requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True,
            verify=False,
            stream=True,
        ) as resp:
            content_type = resp.headers.get("Content-Type", "").lower()
            chunks = []
            total = 0
            first_byte_time = None
            for chunk in resp.iter_content(chunk_size=32 * 1024):
                if not chunk:
                    break
                if first_byte_time is None:
                    first_byte_time = time.perf_counter()
                chunks.append(chunk)
                total += len(chunk)
                if total >= limit_bytes:
                    break
                if time.perf_counter() - start >= SINGLE_URL_MAX_SECONDS:
                    break
            ok = 200 <= resp.status_code < 400 and total > 0
            text = b"".join(chunks).decode(resp.encoding or "utf-8", errors="ignore")

            now = time.perf_counter()
            if first_byte_time is not None:
                connect_time = first_byte_time - start
                download_time = now - first_byte_time
            else:
                connect_time = now - start
                download_time = 0.0

            return ok, text, content_type, total, connect_time, download_time
    except Exception:
        return False, "", "", 0, time.perf_counter() - start, 0.0


def test_direct_bytes(url: str, limit_bytes: int) -> dict:
    """
    测试直链流，返回详细信息:
    ok, bytes, connect_time, download_time, speed_kbps, content_type, format
    """
    start = time.perf_counter()
    headers = dict(DEFAULT_HEADERS)
    headers["Range"] = f"bytes=0-{max(limit_bytes - 1, 0)}"
    try:
        with requests.get(
            url,
            headers=headers,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True,
            verify=False,
            stream=True,
        ) as resp:
            content_type = resp.headers.get("Content-Type", "").lower()
            total, connect_time, download_time, first_bytes = read_limited_response(
                resp, limit_bytes, start, SINGLE_URL_MAX_SECONDS
            )
            fmt = detect_content_format(content_type, first_bytes)
            status_ok = 200 <= resp.status_code < 400
            html_like = fmt == "html" and total < MIN_VALID_BYTES
            ok = status_ok and total >= MIN_VALID_BYTES and not html_like

            # 纯下载速度（排除连接耗时）
            pure_download_time = max(download_time, 0.001)
            speed_kbps = total / 1024 / pure_download_time

            return {
                "ok": ok,
                "bytes": total,
                "connect_time": round(connect_time, 4),
                "download_time": round(download_time, 4),
                "speed_kbps": round(speed_kbps, 2),
                "content_type": content_type,
                "format": fmt,
            }
    except Exception:
        return {
            "ok": False,
            "bytes": 0,
            "connect_time": round(time.perf_counter() - start, 4),
            "download_time": 0.0,
            "speed_kbps": 0.0,
            "content_type": "",
            "format": "unknown",
        }


def test_m3u8(url: str) -> dict:
    """
    测试 m3u8 直播源，返回详细信息（含从 master playlist 解析的质量信息）。
    """
    result = {
        "ok": False,
        "bytes": 0,
        "connect_time": 0.0,
        "download_time": 0.0,
        "speed_kbps": 0.0,
        "content_type": "",
        "format": "m3u8",
        "reason": "",
        # 质量信息（从 m3u8 元数据解析，非 ffprobe）
        "m3u8_bandwidth": 0,
        "m3u8_width": 0,
        "m3u8_height": 0,
        "m3u8_codecs": "",
    }

    playlist_ok, text, content_type, playlist_bytes, conn_t, dl_t = fetch_text_limited(url)
    result["connect_time"] = round(conn_t, 4)
    result["download_time"] = round(dl_t, 4)
    result["content_type"] = content_type
    result["bytes"] = playlist_bytes

    if not playlist_ok or "#EXTM3U" not in text:
        result["reason"] = "invalid_playlist"
        return result

    # 解析 master playlist 变体流质量信息
    variants = parse_m3u8_variants(url, text)
    if variants:
        best = variants[0]
        result["m3u8_bandwidth"] = best["bandwidth"]
        result["m3u8_width"] = best["width"]
        result["m3u8_height"] = best["height"]
        result["m3u8_codecs"] = best["codecs"]

    child_playlists, segments = extract_m3u8_links(url, text)

    # 如果是 master playlist（有子 playlist 无分片），选择最高质量变体
    if child_playlists and not segments:
        # 优先选已解析的最高质量变体 URL
        child_url = variants[0]["url"] if variants else child_playlists[0]
        child_ok, child_text, child_type, child_bytes, child_conn, child_dl = fetch_text_limited(child_url)
        result["bytes"] += child_bytes
        result["connect_time"] = round(result["connect_time"] + child_conn, 4)
        result["download_time"] = round(result["download_time"] + child_dl, 4)
        result["content_type"] = child_type or content_type
        if child_ok and "#EXTM3U" in child_text:
            _, segments = extract_m3u8_links(child_url, child_text)
        else:
            result["reason"] = "child_playlist_failed"
            return result

    if not segments:
        result["reason"] = "no_segment"
        return result

    # 测试真实分片
    tested_bytes = result["bytes"]
    tested_dl_time = result["download_time"]
    segment_success = 0
    for segment in segments[:M3U8_SEGMENT_TEST_COUNT]:
        seg_result = test_direct_bytes(segment, SEGMENT_READ_BYTES)
        tested_bytes += seg_result["bytes"]
        tested_dl_time += seg_result["download_time"]
        result["content_type"] = seg_result["content_type"] or result["content_type"]
        result["format"] = seg_result["format"] or result["format"]
        if seg_result["ok"]:
            segment_success += 1

    result["bytes"] = tested_bytes
    result["download_time"] = round(tested_dl_time, 4)
    pure_dl = max(tested_dl_time, 0.001)
    result["speed_kbps"] = round(tested_bytes / 1024 / pure_dl, 2)
    result["ok"] = segment_success > 0
    result["reason"] = "ok_m3u8" if result["ok"] else "segment_failed"
    return result


def test_single_url(url: str, url_keywords: List[str], history: dict) -> dict:
    """阶段一：HTTP 快速测速"""
    if is_black_url(url, url_keywords):
        return {
            "url": url, "ok": False, "blocked": True,
            "latency": 0.0, "connect_time": 0.0, "download_time": 0.0,
            "bytes": 0, "speed_kbps": 0.0, "score": 0.0,
            "reason": "url_blacklist", "content_type": "", "format": "",
            "m3u8_bandwidth": 0, "m3u8_width": 0, "m3u8_height": 0, "m3u8_codecs": "",
            "quality_source": "none", "width": 0, "height": 0, "codec": "", "bitrate": 0,
        }

    parsed_path = urlparse(url).path.lower()
    is_m3u8_url = ".m3u8" in parsed_path

    if is_m3u8_url:
        test_result = test_m3u8(url)
    else:
        test_result = test_direct_bytes(url, DIRECT_READ_BYTES)
        if not test_result["ok"] and ("mpegurl" in test_result.get("content_type", "") or "m3u8" in test_result.get("content_type", "")):
            m3u8_result = test_m3u8(url)
            if m3u8_result["ok"]:
                test_result = m3u8_result

    ok = test_result["ok"]
    latency = test_result["connect_time"]
    speed_kbps = test_result["speed_kbps"]

    # 如果 m3u8 已解析出分辨率，用它做初步质量分
    m3u8_h = test_result.get("m3u8_height", 0)
    m3u8_w = test_result.get("m3u8_width", 0)
    m3u8_bw = test_result.get("m3u8_bandwidth", 0)

    # 分辨率过滤：m3u8 元数据已知且低于 MIN_HEIGHT → 直接淘汰
    low_res_reason = ""
    if MIN_HEIGHT > 0 and m3u8_h > 0 and m3u8_h < MIN_HEIGHT:
        ok = False
        low_res_reason = f"low_resolution_{m3u8_h}p"

    # 初步质量分（仅 m3u8 元数据，ffprobe 精测在阶段二）
    preliminary_quality = quality_score(m3u8_w, m3u8_h, m3u8_bw, "")

    speed_score = min(100.0, speed_kbps / 10.0)
    latency_score = max(0.0, 100.0 - latency * 25.0)
    stable_score = history_score(url, history)
    availability_score = 100.0 if ok else 0.0

    score = (
        availability_score * W_AVAILABILITY
        + preliminary_quality * W_QUALITY
        + speed_score * W_SPEED
        + latency_score * W_LATENCY
        + stable_score * W_STABILITY
    )

    final_reason = low_res_reason or test_result.get("reason", "ok_direct" if ok else "direct_failed")

    return {
        "url": url,
        "ok": ok,
        "blocked": False,
        "latency": round(latency, 4),
        "connect_time": test_result.get("connect_time", 0.0),
        "download_time": test_result.get("download_time", 0.0),
        "bytes": test_result.get("bytes", 0),
        "speed_kbps": round(speed_kbps, 2),
        "score": round(score, 2),
        "reason": final_reason,
        "content_type": test_result.get("content_type", ""),
        "format": test_result.get("format", ""),
        "m3u8_bandwidth": m3u8_bw,
        "m3u8_width": m3u8_w,
        "m3u8_height": m3u8_h,
        "m3u8_codecs": test_result.get("m3u8_codecs", ""),
        "quality_source": "m3u8_meta" if m3u8_h > 0 else "none",
        "width": m3u8_w,
        "height": m3u8_h,
        "codec": "",
        "bitrate": m3u8_bw,
    }


# ====================== ffprobe 精测（阶段二） ======================
def ffprobe_url(url: str) -> Optional[dict]:
    """
    用 ffprobe 获取流媒体真实参数。
    返回: {width, height, codec, bitrate, fps, format_name} 或 None
    """
    if not FFPROBE_PATH:
        return None

    cmd = [
        FFPROBE_PATH,
        "-v", "error",
        "-show_streams",
        "-show_format",
        "-of", "json",
        "-rw_timeout", str(FFPROBE_TIMEOUT * 1000000),
        url,
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT + 2,
        )
        if proc.returncode != 0:
            return None

        data = json.loads(proc.stdout)
        streams = data.get("streams", [])
        format_info = data.get("format", {})

        video_stream = None
        for s in streams:
            if s.get("codec_type") == "video":
                video_stream = s
                break

        if not video_stream:
            return None

        # 解析帧率
        fps = 0.0
        avg_fps = video_stream.get("avg_frame_rate", "0/1")
        if "/" in avg_fps:
            num, den = avg_fps.split("/")
            den_val = int(den) if int(den) else 1
            fps = round(int(num) / den_val, 2)

        bitrate = int(video_stream.get("bit_rate", 0) or format_info.get("bit_rate", 0) or 0)

        return {
            "width": int(video_stream.get("width", 0)),
            "height": int(video_stream.get("height", 0)),
            "codec": video_stream.get("codec_name", ""),
            "bitrate": bitrate,
            "fps": fps,
            "format_name": format_info.get("format_name", ""),
        }
    except subprocess.TimeoutExpired:
        return None
    except Exception:
        return None


def probe_top_candidates(
    channel_results: Dict[str, List[dict]],
    top_n: int,
) -> None:
    """
    阶段二：对每频道 HTTP 测速前 top_n 名做 ffprobe 精测。
    直接修改 result dict 中的 quality 字段并重算 score。
    """
    if not FFPROBE_ENABLED or not FFPROBE_PATH:
        print("【ffprobe】未启用或未安装 ffprobe，跳过精测阶段")
        return

    total_probe = 0
    probe_success = 0
    all_candidates = []

    for channel, results in channel_results.items():
        valid = [r for r in results if r["ok"]]
        valid.sort(key=lambda r: (-r["score"], -r["speed_kbps"], r["latency"]))
        for item in valid[:top_n]:
            all_candidates.append((channel, item))

    if not all_candidates:
        print("【ffprobe】无候选需要精测")
        return

    print(f"【ffprobe】开始精测 {len(all_candidates)} 条候选链接（每频道前 {top_n} 名）")

    def probe_task(item: dict) -> Tuple[dict, Optional[dict]]:
        return item, ffprobe_url(item["url"])

    with ThreadPoolExecutor(max_workers=min(8, STREAM_TEST_WORKERS)) as pool:
        futures = {pool.submit(probe_task, item): (ch, item) for ch, item in all_candidates}
        for future in as_completed(futures):
            ch, item = futures[future]
            try:
                _, probe_data = future.result()
            except Exception:
                probe_data = None

            total_probe += 1
            if total_probe % 20 == 0:
                print(f"【ffprobe 进度】{total_probe}/{len(all_candidates)}")

            if probe_data:
                probe_success += 1
                item["width"] = probe_data["width"]
                item["height"] = probe_data["height"]
                item["codec"] = probe_data["codec"]
                item["bitrate"] = probe_data["bitrate"]
                item["fps"] = probe_data.get("fps", 0)
                item["format_name"] = probe_data.get("format_name", "")
                item["quality_source"] = "ffprobe"

                # 分辨率过滤：ffprobe 确认低于 MIN_HEIGHT → 淘汰
                if MIN_HEIGHT > 0 and probe_data["height"] > 0 and probe_data["height"] < MIN_HEIGHT:
                    item["ok"] = False
                    item["reason"] = f"low_resolution_{probe_data['height']}p"
                    item["score"] = 0.0
                    total_probe += 0  # 不影响计数
                    continue

                # 重算综合分（用真实分辨率）
                q_score = quality_score(
                    probe_data["width"],
                    probe_data["height"],
                    probe_data["bitrate"],
                    probe_data["codec"],
                )
                speed_score = min(100.0, item["speed_kbps"] / 10.0)
                latency_score = max(0.0, 100.0 - item["latency"] * 25.0)
                stable_score = history_score(item["url"], _history_ref[0])
                availability_score = 100.0

                item["score"] = round(
                    availability_score * W_AVAILABILITY
                    + q_score * W_QUALITY
                    + speed_score * W_SPEED
                    + latency_score * W_LATENCY
                    + stable_score * W_STABILITY,
                    2,
                )

    print(f"【ffprobe】精测完成：成功 {probe_success}/{total_probe}")


# ====================== 持续稳定性测试（可选阶段二.5） ======================
def sustained_stability_test(url: str, seconds: float) -> Tuple[bool, float, int]:
    """
    持续拉流 N 秒，检测是否断流。
    返回: (是否稳定, 平均速度 kbps, 总字节)
    """
    start = time.perf_counter()
    headers = dict(DEFAULT_HEADERS)
    headers["Range"] = "bytes=0-"
    try:
        with requests.get(
            url,
            headers=headers,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True,
            verify=False,
            stream=True,
        ) as resp:
            if not (200 <= resp.status_code < 400):
                return False, 0.0, 0

            total = 0
            last_check = start
            last_bytes = 0
            stall_count = 0

            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    break
                total += len(chunk)
                now = time.perf_counter()
                elapsed = now - start

                if now - last_check >= 1.0:
                    # 每秒检查一次，如果 1 秒内没有新增数据，记为 stall
                    if total - last_bytes < 1024:
                        stall_count += 1
                    last_bytes = total
                    last_check = now

                if elapsed >= seconds:
                    break

            avg_speed = total / 1024 / max(time.perf_counter() - start, 0.001)
            stable = stall_count <= 1 and total > MIN_VALID_BYTES
            return stable, round(avg_speed, 2), total
    except Exception:
        return False, 0.0, 0


def stability_test_top_candidates(
    channel_results: Dict[str, List[dict]],
    top_n: int,
    seconds: float,
) -> None:
    """对每频道前 top_n 名做持续拉流稳定性测试"""
    if seconds <= 0:
        return

    total_test = 0
    all_candidates = []
    for channel, results in channel_results.items():
        valid = [r for r in results if r["ok"]]
        valid.sort(key=lambda r: (-r["score"], -r["speed_kbps"]))
        for item in valid[:top_n]:
            all_candidates.append(item)

    if not all_candidates:
        return

    print(f"【稳定性测试】对 {len(all_candidates)} 条候选做 {seconds:.0f} 秒持续拉流")

    for item in all_candidates:
        total_test += 1
        stable, avg_speed, total_bytes = sustained_stability_test(item["url"], seconds)
        item["sustained_stable"] = stable
        item["sustained_speed_kbps"] = avg_speed
        item["sustained_bytes"] = total_bytes

        # 不稳定的链接降分
        if not stable:
            item["score"] = round(item["score"] * 0.5, 2)
            item["reason"] = "unstable_sustained"

        if total_test % 10 == 0:
            print(f"【稳定性测试进度】{total_test}/{len(all_candidates)}")

    print(f"【稳定性测试】完成 {total_test} 条")


# ====================== 评分模型 ======================
def quality_score(width: int, height: int, bitrate: int, codec: str) -> float:
    """
    清晰度评分（0-100+）。
    720P 以下已在测速阶段被淘汰，此函数只对 720P+ 评分。
    """
    # 基础分辨率分
    if height >= 2160:
        base = 95
    elif height >= 1080:
        base = 85
    elif height >= 720:
        base = 70
    elif height > 0:
        # 低于 720 的不应到达这里，但保留兼容
        base = 40
    else:
        base = 50  # 未知分辨率（可能是直链 ts，ffprobe 未跑），给中等分不惩罚

    # 码率加分（每 2Mbps 加 2 分，上限 +12）
    if bitrate > 0:
        bw_bonus = min(12.0, bitrate / 2_000_000 * 2.0)
        base += bw_bonus

    # 编码加分
    codec_lower = codec.lower()
    if "h265" in codec_lower or "hevc" in codec_lower:
        base += 5  # 同码率下 H.265 画质更好
    elif "h264" in codec_lower or "avc" in codec_lower:
        base += 2

    return max(0.0, base)


def history_score(url: str, history: dict) -> float:
    """
    历史稳定性评分（0-100），加入时间衰减。
    最近的成功比旧的成功权重更高。
    """
    item = history.get("urls", {}).get(url, {})
    success = int(item.get("success", 0))
    fail = int(item.get("fail", 0))
    total = success + fail
    if total == 0:
        return 60.0  # 新链接给中等偏上初始分

    base_rate = success / total * 100

    # 最近一次失败且最近没有成功 → 降分
    last_success = item.get("last_success", "")
    last_fail = item.get("last_fail", "")
    if last_fail and (not last_success or last_fail > last_success):
        base_rate *= 0.7  # 最近失败未恢复，降 30%

    # 成功次数太少时不稳定，适当降分
    if success < 3:
        base_rate *= 0.85

    return max(0.0, min(100.0, base_rate))


# 全局历史引用（供 probe_top_candidates 内部使用）
_history_ref: List[dict] = []


def update_history(history: dict, results: List[dict]) -> None:
    url_history = history.setdefault("urls", {})
    ts = now_iso()
    for result in results:
        url = result["url"]
        item = url_history.setdefault(
            url,
            {
                "success": 0,
                "fail": 0,
                "last_success": "",
                "last_fail": "",
                "last_score": 0,
            },
        )
        if result["ok"]:
            item["success"] = int(item.get("success", 0)) + 1
            item["last_success"] = ts
        else:
            item["fail"] = int(item.get("fail", 0)) + 1
            item["last_fail"] = ts
        item["last_score"] = result["score"]
        item["last_speed_kbps"] = result.get("speed_kbps", 0)
        item["last_latency"] = result.get("latency", 0)
        if result.get("height"):
            item["last_height"] = result["height"]
            item["last_codec"] = result.get("codec", "")
    history["updated_at"] = ts


# ====================== 候选选择 ======================
def select_best_links(results: List[dict], keep_count: int) -> List[dict]:
    # 720P 过滤兜底：已知分辨率且低于阈值的直接排除
    valid = [r for r in results if r["ok"]]
    if MIN_HEIGHT > 0:
        valid = [
            r for r in valid
            if not (r.get("height", 0) > 0 and r.get("height", 0) < MIN_HEIGHT)
        ]
    # 综合分降序 → 速度降序 → 延迟升序 → 清晰度降序
    valid.sort(
        key=lambda r: (
            -r["score"],
            -r.get("speed_kbps", 0),
            r.get("latency", 999),
            -r.get("height", 0),
        )
    )
    if len(valid) <= keep_count:
        return valid

    # 优先不同 host（CDN 多样性）
    selected = []
    used_hosts = set()
    for item in valid:
        h = host_of(item["url"])
        if h not in used_hosts:
            selected.append(item)
            used_hosts.add(h)
        if len(selected) >= keep_count:
            return selected

    # 不够再从剩余里补
    for item in valid:
        if item not in selected:
            selected.append(item)
        if len(selected) >= keep_count:
            break
    return selected


# ====================== 源聚合 ======================
def collect_channels(
    sources: List[str],
    alias_map: Dict[str, str],
    allow_set: set,
    exact_black: set,
    fuzzy_black: List[str],
) -> Tuple[Dict[str, List[str]], Dict[str, set]]:
    raw_channels = defaultdict(list)
    url_sources = defaultdict(set)

    pool = ThreadPoolExecutor(max_workers=SOURCE_DOWNLOAD_WORKERS)
    futures = {pool.submit(download_source, src): src for src in sources}
    pending = set(futures)
    deadline = time.monotonic() + SOURCE_STAGE_MAX_SECONDS
    idx = 0
    try:
        while pending and time.monotonic() < deadline:
            done, pending = wait(pending, timeout=2, return_when=FIRST_COMPLETED)
            if not done:
                print(f"【源下载等待】已完成 {idx}/{len(sources)}，剩余 {len(pending)}")
                continue
            for future in done:
                idx += 1
                try:
                    src, text = future.result()
                except Exception as exc:
                    src = futures.get(future, "unknown")
                    text = ""
                    print(f"【源任务异常】{src} | {str(exc)[:100]}")
                if not text:
                    print(f"【源跳过】{src}")
                    continue
                parsed = parse_source(text)
                line_count = sum(len(v) for v in parsed.values())
                print(f"【源解析】{idx}/{len(sources)} 频道 {len(parsed)} 个，线路 {line_count} 条 | {src}")
                for raw_name, urls in parsed.items():
                    std_name = alias_map.get(raw_name.strip(), raw_name.strip())
                    if not std_name:
                        continue
                    if is_black_channel(std_name, exact_black, fuzzy_black):
                        continue
                    if allow_set and std_name not in allow_set:
                        continue
                    for url in urls:
                        clean = clean_url(url)
                        if not clean.startswith(("http://", "https://")):
                            continue
                        raw_channels[std_name].append(clean)
                        url_sources[clean].add(src)
        if pending:
            print(f"【源阶段超时】跳过 {len(pending)} 个未完成源，继续处理已下载内容")
            for future in pending:
                future.cancel()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    channels = {}
    for name, urls in raw_channels.items():
        unique_urls = unique_keep_order(urls)
        if PRETEST_MAX_PER_CHANNEL > 0:
            unique_urls = unique_urls[:PRETEST_MAX_PER_CHANNEL]
        channels[name] = unique_urls
    return channels, url_sources


# ====================== 输出构建 ======================
def build_outputs(
    channel_results: Dict[str, List[dict]],
    template_order: List[str],
    template_info: Dict[str, Tuple[str, str]],
) -> Tuple[str, str, dict]:
    if template_order:
        output_order = template_order
    else:
        output_order = list(channel_results.keys())

    m3u_lines = ['#EXTM3U x-tvg-url="epg.xml.gz"']
    tv_groups = OrderedDict()
    output_channels = 0
    output_links = 0

    for std_name in output_order:
        selected = select_best_links(channel_results.get(std_name, []), KEEP_PER_CHANNEL)
        if not selected:
            continue
        display_name, group_name = template_info.get(std_name, (std_name, "默认分组"))
        tv_groups.setdefault(group_name, [])
        output_channels += 1
        output_links += len(selected)
        for item in selected:
            # 在 M3U 注释中写入清晰度信息，方便播放器识别
            height = item.get("height", 0)
            res_tag = ""
            if height >= 1080:
                res_tag = " [1080P]"
            elif height >= 720:
                res_tag = " [720P]"
            elif height >= 480:
                res_tag = " [480P]"
            m3u_lines.append(f'#EXTINF:-1 group-title="{group_name}",{display_name}{res_tag}')
            m3u_lines.append(item["url"])
            tv_groups[group_name].append(f'{display_name},{item["url"]}')

    tv_lines = []
    for group_name, rows in tv_groups.items():
        tv_lines.append(f"{group_name},#genre#")
        tv_lines.extend(rows)

    summary = {
        "output_channels": output_channels,
        "output_links": output_links,
        "keep_per_channel": KEEP_PER_CHANNEL,
    }
    return "\n".join(m3u_lines) + "\n", "\n".join(tv_lines) + "\n", summary


# ====================== 主流程 ======================
def main() -> int:
    ensure_dirs()
    print("========== IPTV 自动优选（两阶段测速版）开始 ==========")
    started = time.time()

    sources = read_sources()
    if not sources:
        print("【停止】config/sources.txt 为空，请先添加订阅源地址")
        return 1

    alias_map = load_alias()
    allow_set = set(load_lines(ALLOW_LIST_FILE))
    print(f"【配置】白名单频道 {len(allow_set)} 个，留空代表不过滤频道")
    exact_black, fuzzy_black, url_keywords = load_blacklist()
    template_order, template_info = load_template()
    history = load_history()
    _history_ref.clear()
    _history_ref.append(history)

    if FFPROBE_ENABLED and FFPROBE_PATH:
        print(f"【配置】ffprobe 已就绪: {FFPROBE_PATH}，每频道精测前 {FFPROBE_TOP_N} 名")
    else:
        print("【配置】ffprobe 未启用，仅使用 m3u8 元数据做清晰度判断")

    if MIN_HEIGHT > 0:
        print(f"【配置】分辨率过滤: 丢弃 {MIN_HEIGHT}P 以下源")

    print(f"【配置】评分权重: 可用性={W_AVAILABILITY} 清晰度={W_QUALITY} 速度={W_SPEED} 延迟={W_LATENCY} 稳定性={W_STABILITY}")

    # ---------- 源聚合 ----------
    channels, url_sources = collect_channels(sources, alias_map, allow_set, exact_black, fuzzy_black)
    total_urls = sum(len(v) for v in channels.values())
    print(f"【汇总】进入测速频道 {len(channels)} 个，链接 {total_urls} 条")

    # ---------- URL 去重 ----------
    url_to_channels = defaultdict(list)
    unique_urls = []
    for channel, urls in channels.items():
        for url in urls:
            url_to_channels[url].append(channel)
            unique_urls.append(url)
    unique_urls = unique_keep_order(unique_urls)
    original_unique_count = len(unique_urls)
    if MAX_TOTAL_TEST_URLS > 0 and len(unique_urls) > MAX_TOTAL_TEST_URLS:
        unique_urls.sort(key=lambda u: history_score(u, history), reverse=True)
        unique_urls = unique_urls[:MAX_TOTAL_TEST_URLS]
        print(
            f"【测速截断】候选链接 {original_unique_count} 条，"
            f"本次仅测试历史优先的前 {MAX_TOTAL_TEST_URLS} 条"
        )

    # ---------- 阶段一：HTTP 快速测速 ----------
    print(f"【阶段一】HTTP 快速测速 {len(unique_urls)} 条，并发 {STREAM_TEST_WORKERS}")
    url_results = {}
    finished = 0
    timeout_count = 0
    pool = ThreadPoolExecutor(max_workers=STREAM_TEST_WORKERS)
    futures = {pool.submit(test_single_url, url, url_keywords, history): url for url in unique_urls}
    pending = set(futures)
    deadline = time.monotonic() + TEST_STAGE_MAX_SECONDS
    try:
        while pending and time.monotonic() < deadline:
            done, pending = wait(pending, timeout=5, return_when=FIRST_COMPLETED)
            if not done:
                print(f"【测速等待】已完成 {finished}/{len(unique_urls)}，剩余 {len(pending)}")
                continue
            for future in done:
                url = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "url": url, "ok": False, "blocked": False,
                        "latency": 0, "connect_time": 0, "download_time": 0,
                        "bytes": 0, "speed_kbps": 0, "score": 0,
                        "reason": f"exception:{str(exc)[:80]}",
                        "content_type": "", "format": "",
                        "m3u8_bandwidth": 0, "m3u8_width": 0, "m3u8_height": 0, "m3u8_codecs": "",
                        "quality_source": "none", "width": 0, "height": 0, "codec": "", "bitrate": 0,
                    }
                url_results[url] = result
                finished += 1
                if finished % 50 == 0 or finished == len(unique_urls):
                    ok_so_far = sum(1 for r in url_results.values() if r["ok"])
                    print(f"【阶段一进度】{finished}/{len(unique_urls)}，有效 {ok_so_far}")
        if pending:
            timeout_count = len(pending)
            print(f"【阶段一超时】跳过 {timeout_count} 条未完成链接，使用已完成结果继续")
            for future in pending:
                url = futures[future]
                future.cancel()
                url_results[url] = {
                    "url": url, "ok": False, "blocked": False,
                    "latency": 0, "connect_time": 0, "download_time": 0,
                    "bytes": 0, "speed_kbps": 0, "score": 0,
                    "reason": "stage_timeout", "content_type": "", "format": "",
                    "m3u8_bandwidth": 0, "m3u8_width": 0, "m3u8_height": 0, "m3u8_codecs": "",
                    "quality_source": "none", "width": 0, "height": 0, "codec": "", "bitrate": 0,
                }
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    # ---------- 汇总阶段一结果 ----------
    channel_results = defaultdict(list)
    for url, result in url_results.items():
        result_with_source = dict(result)
        result_with_source["sources"] = sorted(url_sources.get(url, []))
        for channel in url_to_channels.get(url, []):
            channel_results[channel].append(result_with_source)

    all_results = list(url_results.values())
    update_history(history, all_results)

    ok_count = sum(1 for r in all_results if r["ok"])
    print(f"【阶段一完成】有效链接 {ok_count}/{len(unique_urls)}")

    # ---------- 阶段二：ffprobe 精测 ----------
    if FFPROBE_ENABLED and FFPROBE_PATH and ok_count > 0:
        print(f"【阶段二】ffprobe 精测分辨率/码率/编码（每频道前 {FFPROBE_TOP_N} 名）")
        probe_top_candidates(channel_results, FFPROBE_TOP_N)
    else:
        print("【阶段二】跳过 ffprobe 精测")

    # ---------- 阶段二.5：持续稳定性测试（可选） ----------
    if SUSTAINED_TEST_SECONDS > 0 and ok_count > 0:
        print(f"【阶段二.5】持续稳定性测试 {SUSTAINED_TEST_SECONDS:.0f} 秒（每频道前 {SUSTAINED_TEST_TOP_N} 名）")
        stability_test_top_candidates(channel_results, SUSTAINED_TEST_TOP_N, SUSTAINED_TEST_SECONDS)

    # ---------- 输出 ----------
    m3u_text, tv_text, output_summary = build_outputs(channel_results, template_order, template_info)
    atomic_write_text(OUTPUT_M3U, m3u_text)
    atomic_write_text(OUTPUT_TV, tv_text)
    atomic_write_json(HISTORY_FILE, history)

    # ---------- 报告 ----------
    # 统计清晰度分布
    height_dist = defaultdict(int)
    low_res_dropped = 0
    for r in all_results:
        if r.get("reason", "").startswith("low_resolution"):
            low_res_dropped += 1
        if r["ok"] and r.get("height"):
            h = r["height"]
            if h >= 1080:
                height_dist["1080P+"] += 1
            elif h >= 720:
                height_dist["720P"] += 1
            else:
                height_dist["other"] += 1

    # ffprobe 后重新统计 ok 数
    final_ok_count = sum(1 for r in all_results if r["ok"])
    ffprobe_count = sum(1 for r in all_results if r.get("quality_source") == "ffprobe")
    m3u8_meta_count = sum(1 for r in all_results if r.get("quality_source") == "m3u8_meta")

    report = {
        "generated_at": now_iso(),
        "duration_seconds": round(time.time() - started, 2),
        "source_count": len(sources),
        "channel_count": len(channels),
        "tested_url_count": len(unique_urls),
        "candidate_url_count": original_unique_count,
        "stage_timeout_url_count": timeout_count,
        "valid_url_count": final_ok_count,
        "low_resolution_dropped": low_res_dropped,
        "ffprobe_tested_count": ffprobe_count,
        "m3u8_meta_quality_count": m3u8_meta_count,
        "height_distribution": dict(height_dist),
        **output_summary,
        "settings": {
            "stream_test_workers": STREAM_TEST_WORKERS,
            "keep_per_channel": KEEP_PER_CHANNEL,
            "pretest_max_per_channel": PRETEST_MAX_PER_CHANNEL,
            "max_total_test_urls": MAX_TOTAL_TEST_URLS,
            "source_stage_max_seconds": SOURCE_STAGE_MAX_SECONDS,
            "test_stage_max_seconds": TEST_STAGE_MAX_SECONDS,
            "single_url_max_seconds": SINGLE_URL_MAX_SECONDS,
            "source_max_bytes": SOURCE_MAX_BYTES,
            "direct_read_bytes": DIRECT_READ_BYTES,
            "segment_read_bytes": SEGMENT_READ_BYTES,
            "min_valid_bytes": MIN_VALID_BYTES,
            "m3u8_segment_test_count": M3U8_SEGMENT_TEST_COUNT,
            "min_height": MIN_HEIGHT,
            "ffprobe_enabled": FFPROBE_ENABLED and bool(FFPROBE_PATH),
            "ffprobe_top_n": FFPROBE_TOP_N,
            "ffprobe_timeout": FFPROBE_TIMEOUT,
            "sustained_test_seconds": SUSTAINED_TEST_SECONDS,
            "weights": {
                "availability": W_AVAILABILITY,
                "quality": W_QUALITY,
                "speed": W_SPEED,
                "latency": W_LATENCY,
                "stability": W_STABILITY,
            },
        },
        "channels": {
            channel: select_best_links(results, KEEP_PER_CHANNEL)
            for channel, results in channel_results.items()
        },
    }
    atomic_write_json(REPORT_JSON, report)

    print("========== IPTV 自动优选完成 ==========")
    print(f"有效链接：{final_ok_count}/{len(unique_urls)}")
    if low_res_dropped > 0:
        print(f"低分辨率淘汰：{low_res_dropped} 条（<{MIN_HEIGHT}P）")
    print(f"ffprobe 精测：{ffprobe_count} 条")
    print(f"清晰度分布：{dict(height_dist) if height_dist else '未检测到分辨率信息'}")
    print(f"输出频道：{output_summary['output_channels']} 个")
    print(f"输出线路：{output_summary['output_links']} 条")
    print(f"标准 M3U：{OUTPUT_M3U.relative_to(ROOT)}")
    print(f"DIYP/TvBox：{OUTPUT_TV.relative_to(ROOT)}")
    print(f"检测报告：{REPORT_JSON.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("【致命异常】")
        print(traceback.format_exc())
        raise SystemExit(1)
