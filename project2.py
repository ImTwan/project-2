import os
import json
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
import random
from datetime import timedelta
import signal
import sys

# ===== CẤU HÌNH =====
PRODUCT_ID_FILE = "products-0-200000(in).csv"
PRODUCT_DATA_DIR = "json_products"
CHECKPOINT_FILE = "checkpoint_done_ids.json"
SUMMARY_FILE = "crawl_summary.txt"
NOT_FOUND_LOG = "not_found_404.txt"
PID_FILE = "crawler.pid"
DUPLICATE_LOG = "duplicate_ids.txt"
CLEANED_PRODUCT_ID_FILE = "products_cleaned.csv"

BATCH_SIZE = 1000
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
PRODUCT_URL_TEMPLATE = "https://api.tiki.vn/product-detail/api/v1/products/{}"

STATUS_SUCCESS = 200
STATUS_NOT_FOUND = 404
STATUS_ERROR = -1

MAX_WORKERS = 30
REQUEST_TIMEOUT = 10
MAX_RETRIES = 5

os.makedirs(PRODUCT_DATA_DIR, exist_ok=True)

# ===== PID LOCK =====
def save_pid():
    pid = os.getpid()
    with open(PID_FILE, "w") as f:
        f.write(str(pid))
    print(f"💾 Đã lưu PID: {pid} vào file {PID_FILE}")

def get_existing_pid():
    try:
        with open(PID_FILE, "r") as f:
            return int(f.read().strip())
    except:
        return None

def is_already_running():
    pid = get_existing_pid()
    if pid:
        try:
            os.kill(pid, 0)
            print(f"⚠️ Script đã chạy trước đó với PID: {pid}")
            return True
        except ProcessLookupError:
            print(f"ℹ️ PID {pid} không còn chạy. Có thể là file PID cũ.")
            return False
        except Exception as e:
            print(f"⚠️ Lỗi khi kiểm tra PID {pid}: {e}")
            return False
    return False

def clean_pid():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
        print(f"🧹 Đã xóa file PID: {PID_FILE}")

# ===== CHECKPOINT =====
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try:
                return set(json.load(f))
            except:
                return set()
    return set()

def save_checkpoint(done_ids):
    with open(CHECKPOINT_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(list(done_ids), f)
    os.replace(CHECKPOINT_FILE + ".tmp", CHECKPOINT_FILE)

# ===== DUPLICATE CHECK & CLEAN =====
def check_and_remove_duplicates(input_file, output_file, duplicate_log_file):
    print(f"🔍 Đang xử lý file input: {input_file}")

    seen = set()
    cleaned = []
    duplicates = set()

    try:
        with open(input_file, "r", encoding="utf-8") as f:
            original_lines = f.readlines()
    except Exception as e:
        print(f"❌ Không thể đọc file {input_file}: {e}")
        raise

    for line in original_lines:
        pid = line.strip()
        if not pid:
            continue
        if pid in seen:
            duplicates.add(pid)
        else:
            seen.add(pid)
            cleaned.append(pid)

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for pid in cleaned:
                f.write(pid + "\n")
    except Exception as e:
        print(f"❌ Không thể ghi file sạch {output_file}: {e}")
        raise

    try:
        with open(duplicate_log_file, "w", encoding="utf-8") as f:
            for dup in sorted(duplicates):
                f.write(dup + "\n")
    except Exception as e:
        print(f"❌ Không thể ghi file duplicate log {duplicate_log_file}: {e}")
        raise

    total_original = len(original_lines)
    total_duplicates = len(duplicates)
    total_cleaned = len(cleaned)

    print("🔍 Kiểm tra trùng lặp sản phẩm:")
    print(f"📄 Tổng số product_id đọc vào: {total_original}")
    print(f"⚠️ Số lượng bị trùng lặp: {total_duplicates}")
    print(f"✅ Số lượng sau khi loại bỏ trùng lặp: {total_cleaned}")

    if duplicates:
        print(f"📝 Đã ghi log các ID trùng vào: {duplicate_log_file}")
    else:
        print(f"📁 Không có ID trùng lặp. File {duplicate_log_file} sẽ rỗng.")

    print(f"📦 File ID sạch đã lưu tại: {output_file}")

    return output_file

# ===== LOAD PRODUCT IDS =====
def load_product_ids():
    try:
        cleaned_file = check_and_remove_duplicates(PRODUCT_ID_FILE, CLEANED_PRODUCT_ID_FILE, DUPLICATE_LOG)
    except Exception as e:
        print(f"❌ Lỗi khi làm sạch dữ liệu: {e}")
        sys.exit(1)

    ids = set()
    with open(cleaned_file, "r", encoding="utf-8") as f:
        for line in f:
            pid = line.strip()
            if pid:
                ids.add(pid)
    return list(ids)

# ===== CRAWL DETAIL =====
def crawl_product_detail(pid):
    url = PRODUCT_URL_TEMPLATE.format(pid)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        except Exception:
            time.sleep(1 + random.random())
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except:
                continue

            if not data or not data.get("id"):
                return None, STATUS_ERROR

            desc_html = data.get("description", "")
            desc = BeautifulSoup(desc_html, "html.parser").get_text(" ", strip=True)
            desc = " ".join(desc.split())

            images = []
            for img in data.get("images", []):
                if img.get("base_url"):
                    images.append(img["base_url"])

            return {
                "id": str(data.get("id")),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": desc,
                "images": images
            }, STATUS_SUCCESS

        elif resp.status_code == 404:
            with open(NOT_FOUND_LOG, "a", encoding="utf-8") as f:
                f.write(pid + "\n")
            return None, STATUS_NOT_FOUND

        elif resp.status_code in (429, 500, 502, 503):
            time.sleep(1 + random.random())
            continue
        else:
            return None, STATUS_ERROR

    return None, STATUS_ERROR

# ===== SAVE BATCH =====
def save_batch(batch, index):
    if not batch:
        return
    out_file = os.path.join(PRODUCT_DATA_DIR, f"products_{index:05}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved batch {index}, {len(batch)} sản phẩm")

# ===== MAIN PROCESS =====
def process_all(product_ids, done_ids):
    batch = []
    file_index = len(os.listdir(PRODUCT_DATA_DIR)) + 1
    success_count = 0
    error_count = 0

    remaining = [pid for pid in product_ids if pid not in done_ids]
    print(f"👉 Tổng sản phẩm: {len(product_ids)} | Đã làm: {len(done_ids)} | Còn lại: {len(remaining)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(crawl_product_detail, pid): pid for pid in remaining}

        for future in concurrent.futures.as_completed(futures):
            pid = futures[future]
            try:
                product, status = future.result()
            except Exception:
                product, status = None, STATUS_ERROR

            if status == STATUS_SUCCESS and product:
                batch.append(product)
                done_ids.add(pid)
                success_count += 1
            elif status == STATUS_NOT_FOUND:
                done_ids.add(pid)
            else:
                error_count += 1

            if len(batch) >= BATCH_SIZE:
                save_batch(batch, file_index)
                file_index += 1
                batch.clear()
                save_checkpoint(done_ids)

    if batch:
        save_batch(batch, file_index)
        save_checkpoint(done_ids)

    return success_count, error_count

# ===== SIGNAL HANDLER =====
def graceful_exit(signum, frame):
    print("\n⚠️ Nhận tín hiệu dừng. Dọn dẹp...")
    clean_pid()
    sys.exit(1)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# ===== MAIN =====
def main():
    if is_already_running():
        sys.exit(1)

    save_pid()
    start_time = time.time()

    try:
        product_ids = load_product_ids()
        print(f"📊 Tổng số product_id sau khi làm sạch: {len(product_ids)}")

        done_ids = load_checkpoint()
        success, error = process_all(product_ids, done_ids)
        elapsed = timedelta(seconds=int(time.time() - start_time))

        with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
            f.write("CRAWL JOB SUMMARY:\n")
            f.write(f"Tổng thời gian crawl {len(product_ids)} sản phẩm: {elapsed}\n")
            f.write("Tổng hợp kết quả theo status:\n")
            f.write(f"- Error: {error}\n")
            f.write(f"- Success (HTTP 200): {success}\n")

        print("✅ Crawl hoàn tất. Xem file crawl_summary.txt")

    finally:
        clean_pid()

if __name__ == "__main__":
    main()
