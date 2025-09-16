import os
import json
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
import random

# ===== CẤU HÌNH =====
product_id_file = "products-0-200000(in).csv"  # Danh sách ID sản phẩm (1 dòng 1 id)
product_data_dir = "json_products"  # Thư mục lưu các file JSON batch
os.makedirs(product_data_dir, exist_ok=True)

product_url = "https://api.tiki.vn/product-detail/api/v1/products/{}"
BATCH_SIZE = 1000  # Mỗi file chứa khoảng 1000 sản phẩm

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}

# ===== ĐỌC DANH SÁCH ID =====
def load_product_ids():
    with open(product_id_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# ===== CRAWL CHI TIẾT SẢN PHẨM =====
def crawl_product_detail(pid, max_retries=3, retry_delay=2):
    url = product_url.format(pid)
    
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                if not data or not data.get("id"):
                    return None

                desc = BeautifulSoup(data.get("description", ""), "html.parser").get_text(" ", strip=True)
                images = [img.get("base_url") for img in data.get("images", []) if img.get("base_url")]

                return {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "url_key": data.get("url_key"),
                    "price": data.get("price"),
                    "description": desc,
                    "images": images
                }

            elif resp.status_code == 404:
                print(f"[{pid}] ❌ Không tìm thấy (404)")
                # Ghi lại vào file log
                with open("not_found_404.txt", "a", encoding="utf-8") as log_file:
                    log_file.write(f"{pid}\n")
                return None

            elif resp.status_code in [502, 500, 503]:
                print(f"[{pid}] ⚠️ Server error {resp.status_code}, thử lại ({attempt + 1}/{max_retries})...")
                time.sleep(retry_delay + random.uniform(0, 1))

            else:
                print(f"[{pid}] ❌ Lỗi response không xác định: {resp.status_code}")
                return None

        except Exception as e:
            print(f"[{pid}] ❌ Lỗi khi crawl: {e} (thử lại {attempt + 1}/{max_retries})")
            time.sleep(retry_delay)

    print(f"[{pid}] ❌ Thử lại thất bại sau {max_retries} lần")
    return None


# ===== LƯU THEO BATCH =====
def save_batch(batch, index):
    if not batch:
        return
    out_file = os.path.join(product_data_dir, f"products_{index:04}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã lưu {len(batch)} sản phẩm vào {out_file}")

# ===== XỬ LÝ DANH SÁCH ID =====
def process_products(product_ids):
    batch = []
    file_index = 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(crawl_product_detail, pid): pid for pid in product_ids}

        for future in concurrent.futures.as_completed(futures):
            product = future.result()
            if product:
                batch.append(product)
            if len(batch) >= BATCH_SIZE:
                save_batch(batch, file_index)
                batch = []
                file_index += 1

        # Lưu phần còn lại
        if batch:
            save_batch(batch, file_index)

# ===== CHẠY CHƯƠNG TRÌNH =====
if __name__ == "__main__":
    product_ids = load_product_ids()
    print(f"Tổng số sản phẩm: {len(product_ids)}")
    process_products(product_ids)
