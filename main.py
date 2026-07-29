import json
import os
import re
import time
import traceback
from collections import OrderedDict, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


SOURCE_DOWNLOAD_WORKERS = int(os.getenv("SOURCE_DOWNLOAD_WORKERS", "6"))
STREAM_TEST_WORKERS = int(os.getenv("STREAM_TEST_WORKERS", "10"))
KEEP_PER_CHANNEL = int(os.getenv("KEEP_PER_CHANNEL", "3"))
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
        return {"version": 1, "urls": {}}
    try:
        data = json.loads(text)
        if "urls" not in data:
            data["urls"] = {}
        return data
    except Exception:
        print("【警告】history.json 解析失败，将重新生成历史记录")
        return {"version": 1, "urls": {}}


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
                    print(f"【源截断】{url} 超过 {SOURCE_MAX_BYTES // 1024}KB，仅解析前半部分")
                    break
                if time.perf_counter() - start >= SINGLE_URL_MAX_SECONDS:
                    print(f"【源超时截断】{url} 下载超过 {SINGLE_URL_MAX_SECONDS:.0f} 秒，仅解析已下载内容")
                    break
            return url, b"".join(chunks).decode(resp.encoding or "utf-8", errors="ignore")
    except Exception as exc:
        print(f"【源下载失败】{url} | {str(exc)[:100]}")
        return url, ""


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


def read_limited_response(resp: requests.Response, limit_bytes: int, start: float, max_seconds: float) -> int:
    total = 0
    for chunk in resp.iter_content(chunk_size=64 * 1024):
        if not chunk:
            break
        total += len(chunk)
        if total >= limit_bytes:
            break
        if time.perf_counter() - start >= max_seconds:
            break
    return total


def fetch_text_limited(url: str, limit_bytes: int = 256 * 1024) -> Tuple[bool, str, str, int, float]:
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
            for chunk in resp.iter_content(chunk_size=32 * 1024):
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= limit_bytes:
                    break
                if time.perf_counter() - start >= SINGLE_URL_MAX_SECONDS:
                    break
            ok = 200 <= resp.status_code < 400 and total > 0
            text = b"".join(chunks).decode(resp.encoding or "utf-8", errors="ignore")
            return ok, text, content_type, total, time.perf_counter() - start
    except Exception:
        return False, "", "", 0, time.perf_counter() - start


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


def test_direct_bytes(url: str, limit_bytes: int) -> Tuple[bool, int, float, str]:
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
            total = read_limited_response(resp, limit_bytes, start, SINGLE_URL_MAX_SECONDS)
            status_ok = 200 <= resp.status_code < 400
            html_like = "text/html" in content_type and total < MIN_VALID_BYTES
            ok = status_ok and total >= MIN_VALID_BYTES and not html_like
            return ok, total, time.perf_counter() - start, content_type
    except Exception:
        return False, 0, time.perf_counter() - start, ""


def test_m3u8(url: str) -> Tuple[bool, int, float, str, str]:
    playlist_ok, text, content_type, playlist_bytes, playlist_latency = fetch_text_limited(url)
    if not playlist_ok or "#EXTM3U" not in text:
        return False, playlist_bytes, playlist_latency, content_type, "invalid_playlist"

    child_playlists, segments = extract_m3u8_links(url, text)
    if child_playlists and not segments:
        child_url = child_playlists[0]
        child_ok, child_text, child_type, child_bytes, child_latency = fetch_text_limited(child_url)
        playlist_bytes += child_bytes
        playlist_latency += child_latency
        if child_ok and "#EXTM3U" in child_text:
            _, segments = extract_m3u8_links(child_url, child_text)
            content_type = child_type or content_type

    if not segments:
        return False, playlist_bytes, playlist_latency, content_type, "no_segment"

    tested_bytes = playlist_bytes
    tested_latency = playlist_latency
    segment_success = 0
    for segment in segments[:M3U8_SEGMENT_TEST_COUNT]:
        ok, byte_count, latency, seg_type = test_direct_bytes(segment, SEGMENT_READ_BYTES)
        tested_bytes += byte_count
        tested_latency += latency
        content_type = seg_type or content_type
        if ok:
            segment_success += 1

    ok = segment_success > 0
    reason = "ok_m3u8" if ok else "segment_failed"
    return ok, tested_bytes, tested_latency, content_type, reason


def history_score(url: str, history: dict) -> float:
    item = history.get("urls", {}).get(url, {})
    success = int(item.get("success", 0))
    fail = int(item.get("fail", 0))
    total = success + fail
    if total == 0:
        return 60.0
    return max(0.0, min(100.0, success / total * 100))


def test_single_url(url: str, url_keywords: List[str], history: dict) -> dict:
    if is_black_url(url, url_keywords):
        return {
            "url": url,
            "ok": False,
            "blocked": True,
            "latency": 0.0,
            "bytes": 0,
            "speed_kbps": 0.0,
            "score": 0.0,
            "reason": "url_blacklist",
            "content_type": "",
        }

    parsed_path = urlparse(url).path.lower()
    if ".m3u8" in parsed_path:
        ok, byte_count, latency, content_type, reason = test_m3u8(url)
    else:
        ok, byte_count, latency, content_type = test_direct_bytes(url, DIRECT_READ_BYTES)
        if not ok and ("mpegurl" in content_type or "m3u8" in content_type):
            m3u8_ok, m3u8_bytes, m3u8_latency, m3u8_type, m3u8_reason = test_m3u8(url)
            if m3u8_ok:
                ok, byte_count, latency, content_type, reason = (
                    m3u8_ok,
                    m3u8_bytes,
                    m3u8_latency,
                    m3u8_type,
                    m3u8_reason,
                )
            else:
                reason = "direct_failed"
        else:
            reason = "ok_direct" if ok else "direct_failed"

    speed_kbps = byte_count / 1024 / max(latency, 0.001)
    speed_score = min(100.0, speed_kbps / 10.0)
    latency_score = max(0.0, 100.0 - latency * 20.0)
    stable_score = history_score(url, history)
    availability_score = 100.0 if ok else 0.0
    score = (
        availability_score * 0.40
        + speed_score * 0.25
        + latency_score * 0.15
        + stable_score * 0.20
    )

    return {
        "url": url,
        "ok": ok,
        "blocked": False,
        "latency": round(latency, 4),
        "bytes": byte_count,
        "speed_kbps": round(speed_kbps, 2),
        "score": round(score, 2),
        "reason": reason,
        "content_type": content_type,
    }


def update_history(history: dict, results: List[dict]) -> None:
    url_history = history.setdefault("urls", {})
    ts = now_iso()
    for result in results:
        url = result["url"]
        item = url_history.setdefault(
            url,
            {"success": 0, "fail": 0, "last_success": "", "last_fail": "", "last_score": 0},
        )
        if result["ok"]:
            item["success"] = int(item.get("success", 0)) + 1
            item["last_success"] = ts
        else:
            item["fail"] = int(item.get("fail", 0)) + 1
            item["last_fail"] = ts
        item["last_score"] = result["score"]
        item["last_speed_kbps"] = result["speed_kbps"]
        item["last_latency"] = result["latency"]
    history["updated_at"] = ts


def host_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def select_best_links(results: List[dict], keep_count: int) -> List[dict]:
    valid = [r for r in results if r["ok"]]
    valid.sort(key=lambda r: (-r["score"], -r["speed_kbps"], r["latency"]))
    if len(valid) <= keep_count:
        return valid

    selected = []
    used_hosts = set()
    for item in valid:
        h = host_of(item["url"])
        if h not in used_hosts:
            selected.append(item)
            used_hosts.add(h)
        if len(selected) >= keep_count:
            return selected

    for item in valid:
        if item not in selected:
            selected.append(item)
        if len(selected) >= keep_count:
            break
    return selected


def read_sources() -> List[str]:
    sources = load_lines(SOURCES_FILE)
    print(f"【配置】订阅源 {len(sources)} 个")
    return sources


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
            m3u_lines.append(f'#EXTINF:-1 group-title="{group_name}",{display_name}')
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


def main() -> int:
    ensure_dirs()
    print("========== IPTV 自动优选开始 ==========")
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

    channels, url_sources = collect_channels(sources, alias_map, allow_set, exact_black, fuzzy_black)
    total_urls = sum(len(v) for v in channels.values())
    print(f"【汇总】进入测速频道 {len(channels)} 个，链接 {total_urls} 条")

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

    print(f"【测速】去重后待测链接 {len(unique_urls)} 条，并发 {STREAM_TEST_WORKERS}")
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
                        "url": url,
                        "ok": False,
                        "blocked": False,
                        "latency": 0,
                        "bytes": 0,
                        "speed_kbps": 0,
                        "score": 0,
                        "reason": f"exception:{str(exc)[:80]}",
                        "content_type": "",
                    }
                url_results[url] = result
                finished += 1
                if finished % 50 == 0 or finished == len(unique_urls):
                    print(f"【测速进度】{finished}/{len(unique_urls)}")
        if pending:
            timeout_count = len(pending)
            print(f"【测速阶段超时】跳过 {timeout_count} 条未完成链接，使用已完成结果继续输出")
            for future in pending:
                url = futures[future]
                future.cancel()
                url_results[url] = {
                    "url": url,
                    "ok": False,
                    "blocked": False,
                    "latency": 0,
                    "bytes": 0,
                    "speed_kbps": 0,
                    "score": 0,
                    "reason": "stage_timeout",
                    "content_type": "",
                }
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    channel_results = defaultdict(list)
    for url, result in url_results.items():
        result_with_source = dict(result)
        result_with_source["sources"] = sorted(url_sources.get(url, []))
        for channel in url_to_channels.get(url, []):
            channel_results[channel].append(result_with_source)

    all_results = list(url_results.values())
    update_history(history, all_results)

    m3u_text, tv_text, output_summary = build_outputs(channel_results, template_order, template_info)
    atomic_write_text(OUTPUT_M3U, m3u_text)
    atomic_write_text(OUTPUT_TV, tv_text)
    atomic_write_json(HISTORY_FILE, history)

    ok_count = sum(1 for r in all_results if r["ok"])
    report = {
        "generated_at": now_iso(),
        "duration_seconds": round(time.time() - started, 2),
        "source_count": len(sources),
        "channel_count": len(channels),
        "tested_url_count": len(unique_urls),
        "candidate_url_count": original_unique_count,
        "stage_timeout_url_count": timeout_count,
        "valid_url_count": ok_count,
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
        },
        "channels": {
            channel: select_best_links(results, KEEP_PER_CHANNEL)
            for channel, results in channel_results.items()
        },
    }
    atomic_write_json(REPORT_JSON, report)

    print("========== IPTV 自动优选完成 ==========")
    print(f"有效链接：{ok_count}/{len(unique_urls)}")
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
