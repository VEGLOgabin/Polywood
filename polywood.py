import asyncio
import os
import json
import time
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Setup logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SOURCE_SITE = "https://www.polywood.com"
OUTPUT_FILE = "output/output.json"
DATA = []


def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf8') as f:
            logging.info("Loaded existing data from output.json")
            return json.load(f)
    logging.info("No existing data found.")
    return []


async def get_page_content(page, url):
    try:
        await page.goto(url, timeout=0)
        await page.wait_for_timeout(5000)
        content = await page.content()
        return BeautifulSoup(content, "html.parser")
    except Exception as e:
        logging.error(f"Error loading page {url}: {e}")
        return None


async def get_category_links(page):
    soup = await get_page_content(page, SOURCE_SITE)
    links = []
    if soup:
        anchors = soup.find_all('a', class_="peer pb-sm -mb-sm block")
        for a in anchors:
            href = a.get("href", "")
            if ('/pages/' in href or '/collections/' in href) and 'quick-ship-products' not in href:
                links.append(SOURCE_SITE + href)
        logging.info(f"Found {len(links)} category links.")
    else:
        logging.warning("Failed to load category page.")
    return links


# async def get_product_links(page, category_url):
#     soup = await get_page_content(page, category_url)
#     links = []
#     current = []

#     if not soup:
#         logging.warning(f"Skipping category {category_url} due to missing content.")
#         return [], []

#     try:
#         products = soup.find_all('a', class_='product-card--simple-media__image')
#         if '/collections/' in category_url:
#             current = [soup.find('h1').text.replace('Collection', '').strip()]
#         else:
#             items = soup.find('ul', class_='items').find_all('li', recursive=False)
#             current = [items[-2].text.strip(), items[-1].text.strip()]

#         for product in products:
#             href = "https://www.polywood.com" + product.get("href")
#             print(href)
#             if href:
#                 links.append(href)

#         logging.info(f"Found {len(links)} product links in {category_url}.")
#     except Exception as e:
#         logging.error(f"Error getting product links from {category_url}: {e}")
#     return links, current


async def get_product_links(page, category_url):
    links = []
    current = []
    page_num = 1

    base_url = category_url.split('?')[0]

    while True:
        paginated_url = f"{base_url}?page={page_num}"
        soup = await get_page_content(page, paginated_url)

        if not soup:
            logging.warning(f"Skipping category page {paginated_url} due to missing content.")
            break

        try:
            products = soup.find_all('a', class_='product-card--simple-media__image')
            if not products:
                logging.info(f"No more products found on page {page_num} of {category_url}")
                break

            if page_num == 1:
                if '/collections/' in category_url:
                    h1_tag = soup.find('h1')
                    if h1_tag:
                        current = [h1_tag.text.replace('Collection', '').strip()]
                else:
                    items = soup.find('ul', class_='items').find_all('li', recursive=False)
                    if len(items) >= 2:
                        current = [items[-2].text.strip(), items[-1].text.strip()]

            for product in products:
                href = product.get("href")
                if href:
                    full_url = "https://www.polywood.com" + href
                    # print(full_url)
                    links.append(full_url)

            logging.info(f"Found {len(products)} product links on page {page_num} of {category_url}.")

            # Check if "Next" page exists
            nav = soup.find('nav', role='navigation')
            if not nav or f"?page={page_num + 1}" not in nav.decode():
                break

            page_num += 1
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Error getting product links from {paginated_url}: {e}")
            break

    return links, current



async def get_product_details(page, url, current, scraped_links):
    if url in scraped_links:
        logging.info(f"Skipping already scraped: {url}")
        return None

    logging.info(f"Scraping --> {url}")
    try:
        soup = await get_page_content(page, url)
        if not soup:
            return None

        row = {"Product Link": url}
        row["Title"] = soup.find("h1").text.strip()
        row["SKU"] = soup.find("div", itemprop="sku").text.strip()

        if len(current) == 1:
            row["Collection"] = current[0].upper()
        else:
            row["Main Category"] = current[0]
            row["Collection"] = current[1]

        overview = soup.find(class_='product attribute overview')
        if overview:
            row["Overview"] = overview.text.strip()

        try:
            desc_items = soup.find(class_='product-info-feature-pillars').find_all('li')
            row["Description"] = "\n".join(li.text.strip() for li in desc_items)
        except Exception as e:
            logging.warning(f"Description missing for {url}: {e}")

        try:
            feat_items = soup.find(class_='features').find_all('li')
            row["FEATURES"] = "\n".join(li.text.strip() for li in feat_items)
        except Exception as e:
            logging.warning(f"Features missing for {url}: {e}")

        # Images
        images = []
        thumbs = soup.find('div', attrs={'data-gallery-type': 'thumbnail'})
        if thumbs:
            for img in thumbs.find_all('img'):
                src = img.get("src", "").replace("w_200,h_160,c_fill,q_80", "w_700,h_700,c_pad,q_80") \
                                         .replace("w_100,h_80,c_fill,q_80", "w_700,h_700,c_pad,q_80")
                images.append(src)
        else:
            gallery = soup.find('div', class_='gallery-placeholder')
            for img in gallery.find_all('img'):
                images.append(img.get("src", ""))

        row["Images"] = list(set(images))

        # Dimensions
        try:
            dim_main = soup.find(class_='dimensions one two weight-dimensions')
            row["Overall Dimensions"] = dim_main.find('p').text.split(':')[1].strip()
            trs = dim_main.find_all('tr')
            row["WEIGHT & DIMENSIONS"] = [
                {tr.find_all('td')[0].text.strip(): tr.find_all('td')[1].text.strip()} for tr in trs
            ]
        except Exception as e:
            logging.warning(f"Dimensions missing or invalid for {url}: {e}")

        # Assembly Info
        try:
            for div in soup.find(class_='links').find_all('div'):
                if 'assembly information' in div.text.lower():
                    row['Assembly Information'] = SOURCE_SITE + div.find('a')['href']
                    break
        except Exception as e:
            logging.warning(f"Assembly info missing for {url}: {e}")

        # SKU Options
        try:
            row["SKU Options"] = []
            options = soup.find('div', class_='option-groupings')
            if options:
                for opt in options.find_all('div', class_='grouping-option-value'):
                    row["SKU Options"].append({
                        "SKU": opt.get("option-sku", "").strip(),
                        "Color": opt.get("option-label", "").strip()
                    })
        except Exception as e:
            logging.error(f"Error extracting SKU options from {url}: {e}")

        return row

    except Exception as e:
        logging.error(f"Error scraping product details for {url}: {e}")
        return None


async def main():
    global DATA
    DATA = load_existing_data()
    scraped_links = {d["Product Link"] for d in DATA}

    os.makedirs("output", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        category_links = await get_category_links(page)

        for category_url in category_links:
            product_links, current = await get_product_links(page, category_url)
            for product_url in product_links:
                product_data = await get_product_details(page, product_url, current, scraped_links)
                if product_data:
                    DATA.append(product_data)
                    with open(OUTPUT_FILE, "w", encoding="utf8") as f:
                        json.dump(DATA, f, indent=4, ensure_ascii=False)
                    logging.info(f"Saved product: {product_data['Title']} from {product_url}")
                    await asyncio.sleep(2)

        await browser.close()
        logging.info("Scraping completed.")


if __name__ == "__main__":
    asyncio.run(main())
