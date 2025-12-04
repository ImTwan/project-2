# Tiki Product Data Pipeline (Python Web Crawler)
# Project Overview
Using Python code, download the information for 200k products (product ID list below) from Tiki and save them as .json files. Each file should contain information for approximately 1000 products. The information to be retrieved includes: id, name, url_key, price, description, image URLs. The content in the 'description' should be standardized, and a solution should be found to reduce the data retrieval time.
* List product_id: https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn
* API get product detail: https://api.tiki.vn/product-detail/api/v1/products/138083218
# Result: 
Total time to crawl 200,001 products: 0:58:19. 
Summarize the results by status:
* Error: 8
* Success (HTTP 200): 198,934

