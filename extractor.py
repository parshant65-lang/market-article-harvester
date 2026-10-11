import os
import sys
import re
import time
import json
import shutil
import hashlib
import datetime
import subprocess
import html as html_lib
import urllib.request
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

VAULT_PAT = os.environ.get("VAULT_PAT")
if not VAULT_PAT:
    print("[ERROR] VAULT_PAT environment variable is required.")
    sys.exit(1)

VAULT_REPO_URL = f"https://x-access-token:{VAULT_PAT}@github.com/parshant65-lang/market-intelligence-vault.git"
VAULT_DIR = "vault_storage"
BUFFER_DIR = "local_batch_buffer"

SESSION_MAX_SECONDS = 5 * 3600 + 30 * 60   # 5.5 Hours continuous session before relay handoff
PUSH_INTERVAL_SECONDS = 1800                # Exactly 30 MINUTES push cadence
BATCH_SIZE = 100                            # Process batches of 100 URLs
NUM_WORKERS = 20                            # 20 parallel threads for rapid extraction

BULLISH_WORDS = {
    "surge", "surges", "jump", "jumps", "gain", "gains", "rally", "rallies",
    "record high", "profit", "profits", "beat", "beats", "boom", "booming",
    "upgrade", "upgrades", "rise", "rises", "rising", "soar", "soars",
    "positive", "growth", "rebound", "rebounds", "outperform", "optimism"
}

BEARISH_WORDS = {
    "fall", "falls", "drop", "drops", "plunge", "plunges", "slump", "slumps",
    "loss", "losses", "warning", "warnings", "probe", "probes", "war", "conflict",
    "inflation", "default", "defaults", "crisis", "crash", "downgrade", "downgrades",
    "record low", "recession", "slowdown", "weak", "weakens", "fears", "cut", "cuts"
}

BLACKLIST_PHRASES = [
    "sign up", "privacy policy", "terms of service", "livestream", "watchlist",
    "newsletter", "cookie", "advertisement", "all rights reserved", "subscribe",
    "trending now", "menu", "skip to content"
]

def clean_html(raw_html):
    if not raw_html:
        return ""
    clean = re.sub(r"<[^>]+>", "", raw_html)
    clean = html_lib.unescape(clean)
    return " ".join(clean.split())

def score_sentiment(text):
    t = text.lower()
    bull = sum(1 for w in BULLISH_WORDS if w in t)
    bear = sum(1 for w in BEARISH_WORDS if w in t)
    total = bull + bear
    if total == 0:
        return 0.0, "NEUTRAL"
    score = round((bull - bear) / total, 2)
    label = "BULLISH" if score > 0.15 else ("BEARISH" if score < -0.15 else "NEUTRAL")
    return score, label

def parse_pubdate(pubdate_str):
    if not pubdate_str:
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        dt = parsedate_to_datetime(pubdate_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def extract_article(target):
    url = target["url"]
    source = target["source"]
    category = target["category"]

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode("utf-8", errors="ignore")

        # 1. Title
        title = ""
        og_t = re.search(r"""<meta\s+(?:property|name)=["\x27]og:title["\x27]\s+content=["\x27](.*?)["\x27]""", html)
        if og_t:
            title = clean_html(og_t.group(1))
        if not title:
            h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
            if h1:
                title = clean_html(h1.group(1))
        if not title:
            t = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
            if t:
                title = clean_html(t.group(1))

        # 2. Date
        pub_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        og_time = re.search(r"""<meta\s+(?:property|name)=["\x27](?:article:published_time|date|publishdate|lastmod)["\x27]\s+content=["\x27](.*?)["\x27]""", html, re.IGNORECASE)
        if og_time:
            pub_utc = parse_pubdate(og_time.group(1))
        else:
            time_tag = re.search(r"""<time[^>]+datetime=["\x27](.*?)["\x27]""", html)
            if time_tag:
                pub_utc = parse_pubdate(time_tag.group(1))

        # 3. Clean Paragraphs
        raw_paras = re.findall(r"<p[^>]*>(.*?)</p>", html)
        clean_paras = []
        for p in raw_paras:
            txt = clean_html(p)
            if len(txt) > 70 and not any(bp in txt.lower() for bp in BLACKLIST_PHRASES):
                clean_paras.append(txt)

        if len(clean_paras) >= 3:
            unique_seed = f"{url}::{title}"
            item_id = hashlib.md5(unique_seed.encode("utf-8")).hexdigest()
            sample_text = " ".join(clean_paras[:5])
            sc, lb = score_sentiment(f"{title} {sample_text}")
            article_item = {
                "id": item_id,
                "published_utc": pub_utc,
                "source": source,
                "category": category,
                "title": title,
                "description": clean_paras[0] if clean_paras else "",
                "url": url,
                "full_content": clean_paras,
                "sentiment": {"score": sc, "label": lb},
                "ingested_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }
            return article_item, url, "ingested"
        else:
            return None, url, "skipped"
    except Exception:
        return None, url, "skipped"

def get_registry_parts(vault_data_dir):
    files = []
    if os.path.exists(vault_data_dir):
        for fname in os.listdir(vault_data_dir):
            m = re.match(r"^link_registry_part(\d+)\.json$", fname)
            if m:
                files.append((int(m.group(1)), fname, os.path.join(vault_data_dir, fname)))
    if not files:
        fallback_path = os.path.join(vault_data_dir, "link_registry_part1.json")
        return [(1, "link_registry_part1.json", fallback_path)]
    return sorted(files, key=lambda x: x[0])

def sync_batch_to_vault():
    buffer_file = os.path.join(BUFFER_DIR, "buffered_items.json")
    registry_updates_file = os.path.join(BUFFER_DIR, "registry_updates.json")

    if not os.path.exists(buffer_file) and not os.path.exists(registry_updates_file):
        print("[30-MIN SYNC] Buffer is empty. Zero new items to push.")
        return

    try:
        with open(buffer_file, "r", encoding="utf-8") as f:
            buffered_items = json.load(f)
    except Exception:
        buffered_items = []

    try:
        with open(registry_updates_file, "r", encoding="utf-8") as f:
            registry_updates = json.load(f)
    except Exception:
        registry_updates = {}

    if not buffered_items and not registry_updates:
        print("[30-MIN SYNC] Buffer contains 0 items. Skipping push.")
        return

    print(f"\n[30-MIN SYNC] Initiating 30-Minute Vault Sync: {len(buffered_items)} articles to vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)

    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[30-MIN SYNC ERROR] Failed to connect to vault repository.")
        return

    # 1. Update Link Registry Volumes
    data_dir = os.path.join(VAULT_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    parts = get_registry_parts(data_dir)

    total_ingested_all = 0
    updated_urls = set()

    for part_num, fname, fpath in parts:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                p_data = json.load(f)
        except Exception:
            continue

        p_links = p_data.get("links", {})
        part_modified = False

        for url, info in registry_updates.items():
            if url in p_links:
                p_links[url].update(info)
                updated_urls.add(url)
                part_modified = True

        if part_modified:
            p_data["total_discovered"] = len(p_links)
            p_data["total_ingested"] = sum(1 for v in p_links.values() if v.get("status") == "ingested")
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(p_data, f, indent=2, ensure_ascii=False)

        total_ingested_all += p_data.get("total_ingested", 0)

    # Edge case: Any URL not found in scanned parts goes to latest active part
    unmatched_urls = {u: info for u, info in registry_updates.items() if u not in updated_urls}
    if unmatched_urls:
        latest_num, latest_name, latest_path = parts[-1]
        latest_data = {"part": latest_num, "total_discovered": 0, "total_ingested": 0, "links": {}}
        if os.path.exists(latest_path):
            try:
                with open(latest_path, "r", encoding="utf-8") as f:
                    latest_data = json.load(f)
            except Exception:
                pass
        for u, info in unmatched_urls.items():
            latest_data.setdefault("links", {})[u] = info
        latest_data["total_discovered"] = len(latest_data.get("links", {}))
        latest_data["total_ingested"] = sum(1 for v in latest_data.get("links", {}).values() if v.get("status") == "ingested")
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_data, f, indent=2, ensure_ascii=False)
        total_ingested_all += len(unmatched_urls)

    # Clean up legacy link_registry.json if present
    legacy_file = os.path.join(data_dir, "link_registry.json")
    if os.path.exists(legacy_file):
        try:
            os.remove(legacy_file)
            subprocess.run(["git", "rm", "-f", "data/link_registry.json"], cwd=VAULT_DIR, capture_output=True)
        except Exception:
            pass

    # 2. Partition into Category Date Files
    file_groups = {}
    for item in buffered_items:
        category = item["category"]
        pub_dt = item["published_utc"].split(" ")[0]
        try:
            dt_obj = datetime.datetime.strptime(pub_dt, "%Y-%m-%d")
            year, month = str(dt_obj.year), f"{dt_obj.month:02d}"
        except Exception:
            today = datetime.datetime.now(datetime.timezone.utc)
            year, month = str(today.year), f"{today.month:02d}"

        target_folder = os.path.join(VAULT_DIR, "data", category, year, month)
        os.makedirs(target_folder, exist_ok=True)
        file_path = os.path.join(target_folder, f"{category}_{pub_dt}.json")

        if file_path not in file_groups:
            existing = []
            known_ids = set()
            known_urls = set()
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                    for e in existing:
                        known_ids.add(e.get("id"))
                        known_urls.add(e.get("url"))
                except Exception:
                    existing = []
            file_groups[file_path] = (existing, known_ids, known_urls)

        ex_records, known_ids, known_urls = file_groups[file_path]
        if item["id"] not in known_ids and item["url"] not in known_urls:
            ex_records.append(item)
            known_ids.add(item["id"])
            known_urls.add(item["url"])

    for file_path, (records, _, _) in file_groups.items():
        if records:
            for r in records:
                r.pop("related_links", None)
            records.sort(key=lambda x: x["published_utc"], reverse=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)

    # 3. Pull rebase & Push to Vault
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "add", "data/"], cwd=VAULT_DIR, check=True)
    commit_res = subprocess.run(
        ["git", "commit", "-m", f"Extractor 30-Min sync: {len(buffered_items)} articles + Link Registry ({total_ingested_all} ingested) [skip ci]"],
        cwd=VAULT_DIR, capture_output=True, text=True
    )
    if commit_res.returncode == 0:
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        if push_res.returncode == 0:
            print(f"[30-MIN SYNC] Successfully synced {len(buffered_items)} articles to vault.")
        else:
            print(f"[30-MIN SYNC ERROR] Push failed: {push_res.stderr.strip()}")

    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("[30-MIN SYNC] Local buffer deleted. Runner storage 100% clean.")

def trigger_next_relay():
    url = "https://api.github.com/repos/parshant65-lang/market-article-harvester/actions/workflows/harvest_articles.yml/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {VAULT_PAT}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Continuous-Harvester-Engine"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("[SEAMLESS RELAY] Next continuous harvester session triggered successfully.")
    except Exception as e:
        print(f"[RELAY DISPATCH ERROR] {e}")

def main():
    print("=" * 70)
    print("🚀 AUTONOMOUS FULL-TEXT ARTICLE HARVESTER ENGINE INITIALIZED")
    print(f"   Batch Size: {BATCH_SIZE} URLs parallel ({NUM_WORKERS} ThreadPool Workers)")
    print(f"   Vault Push Cadence: Exactly Every {PUSH_INTERVAL_SECONDS // 60} Minutes")
    print(f"   Max Session Time: {SESSION_MAX_SECONDS // 3600}h {SESSION_MAX_SECONDS % 3600 // 60}m")
    print("=" * 70)

    os.makedirs(BUFFER_DIR, exist_ok=True)
    start_time = time.time()
    last_push_time = time.time()
    total_session_ingested = 0

    while (time.time() - start_time) < SESSION_MAX_SECONDS:
        current_time = time.time()

        # Connect and check pending links from registry volumes
        if os.path.exists(VAULT_DIR):
            shutil.rmtree(VAULT_DIR)
        subprocess.run(["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR], capture_output=True, text=True)

        data_dir = os.path.join(VAULT_DIR, "data")
        parts = get_registry_parts(data_dir)

        # Balanced Multi-Source Round-Robin batch selection (100 URLs across all categories)
        # Scan volumes in ascending order (part1 -> part2 -> part3) to drain older parts first
        by_cat = {}
        for part_num, fname, fpath in parts:
            if os.path.exists(fpath):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        part_data = json.load(f)
                    for url, meta in part_data.get("links", {}).items():
                        if meta.get("status") == "discovered":
                            c = meta.get("category", "cnbc_markets")
                            by_cat.setdefault(c, []).append({
                                "url": url,
                                "source": meta.get("source", "Unknown"),
                                "category": c
                            })
                except Exception:
                    pass
            # Early break once sufficient candidates are collected to build balanced batches
            if sum(len(v) for v in by_cat.values()) >= 500:
                break

        pending_items = []
        categories = list(by_cat.keys())
        while len(pending_items) < BATCH_SIZE and any(by_cat.values()):
            for c in categories:
                if by_cat[c]:
                    item = by_cat[c].pop(0)
                    pending_items.append(item)
                    if len(pending_items) >= BATCH_SIZE:
                        break

        shutil.rmtree(VAULT_DIR, ignore_errors=True)

        if pending_items:
            now_ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
            print(f"\n[{now_ts}] Launching Parallel Extraction: Processing {len(pending_items)} URLs across {len(categories)} categories...")

            batch_articles = []
            status_updates = {}
            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                futures = [executor.submit(extract_article, t) for t in pending_items]
                for f in as_completed(futures):
                    art, url, status = f.result()
                    status_updates[url] = status
                    if art:
                        batch_articles.append(art)

            if batch_articles or status_updates:
                total_session_ingested += len(batch_articles)
                buffer_file = os.path.join(BUFFER_DIR, "buffered_items.json")
                reg_updates_file = os.path.join(BUFFER_DIR, "registry_updates.json")

                existing_buf = []
                if os.path.exists(buffer_file):
                    try:
                        with open(buffer_file, "r", encoding="utf-8") as f:
                            existing_buf = json.load(f)
                    except Exception:
                        existing_buf = []
                existing_buf.extend(batch_articles)

                existing_reg_up = {}
                if os.path.exists(reg_updates_file):
                    try:
                        with open(reg_updates_file, "r", encoding="utf-8") as f:
                            existing_reg_up = json.load(f)
                    except Exception:
                        existing_reg_up = {}

                now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                for art in batch_articles:
                    existing_reg_up[art["url"]] = {
                        "title": art["title"],
                        "source": art["source"],
                        "category": art["category"],
                        "published_utc": art["published_utc"],
                        "ingested_utc": now_iso,
                        "status": "ingested",
                        "has_full_content": True
                    }

                for url, status in status_updates.items():
                    if status == "skipped" and url not in existing_reg_up:
                        existing_reg_up[url] = {"status": "skipped", "updated_utc": now_iso}

                os.makedirs(BUFFER_DIR, exist_ok=True)
                with open(buffer_file, "w", encoding="utf-8") as f:
                    json.dump(existing_buf, f, ensure_ascii=False)
                with open(reg_updates_file, "w", encoding="utf-8") as f:
                    json.dump(existing_reg_up, f, ensure_ascii=False)

                print(f"[{now_ts}] Batch Finished: +{len(batch_articles)} full articles extracted. (Buffered: {len(existing_buf)})")
        else:
            print(f"[{datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S UTC')}] No pending discovered links in queue. Sleeping 30s...")
            time.sleep(30)

        # Check 30-Minute Push Trigger
        if (time.time() - last_push_time) >= PUSH_INTERVAL_SECONDS:
            sync_batch_to_vault()
            last_push_time = time.time()

        time.sleep(10)

    # End of Session Handoff
    print("\n[SESSION COMPLETE] Flushing final buffer and triggering next relay...")
    sync_batch_to_vault()
    trigger_next_relay()
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("🏁 Harvester session handover complete.")

if __name__ == "__main__":
    main()
