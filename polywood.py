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
        # anchors = soup.find_all('a', class_="peer pb-sm -mb-sm block")
        categories = soup.find_all("div", class_ = "w-full block")
        if categories:
            # categories = [item.find("a").text.strip() for item in categories]
            # print(categories)
            for item in categories:
                # print("---------Category link a tag text ----------")
                main_category_name = item.find("a").get_text(strip = True)
                collections_a = item.find_all('a', class_="peer pb-sm -mb-sm block")
                collections_a_link = [[main_category_name,a_link.text.strip(), a_link.get("href")] for a_link in collections_a]
                # print(collections_a_link)
                for data in collections_a_link:
                    if data[0]!='New & Featured' and "View All" not in  data[1] and "Quick Ship" not in data[1]:
                        links.append([[data[0], data[1]], "https://www.polywood.com"+data[2]])

        logging.info(f"Found {len(links)} category links.")
    else:
        logging.warning("Failed to load category page.")
    return links


async def get_product_links(page, category_url, current):
    links = []
    
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
            one_page_product = []
            for product in products:
                href = product.get("href")
                if href:
                    full_url = "https://www.polywood.com" + href
                    one_page_product.append(full_url)
            if len(products)>24:
                one_page_product = list(set(one_page_product)) 

            if one_page_product:
                links.extend(one_page_product)           

            logging.info(f"Found {len(products)} product links on page {page_num} of {category_url}.")
            # break  # For test

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
        print(f"Url : {url}")

        row = {"Product Link": url}
        row["Title"] = soup.find("h1", class_ = "h3").text.strip()
        row["SKU"] = soup.find("p", id = "Sku-template--18905404735715__main").text.strip().replace("SKU", "")
        # print(f"SKU : {row['SKU']}")
        if len(current) == 1:
            row["Collection"] = current[0].upper()
        else:
            row["Main Category"] = current[0]
            row["Collection"] = current[1]


        try:
            desc_span = soup.find('span', class_='metafield-multi_line_text_field')
            row["Description"] = desc_span.get_text(separator=' ', strip=True)
            # print(row["Description"])
        except Exception as e:
            logging.warning(f"Description missing or invalid for {url}: {e}")

        try:
            accordion_div = soup.find('div', id=lambda x: x and x.startswith("accordion-content-collapsible_tab"))
            if accordion_div:
                features_div = accordion_div.find('div', class_='overflow-hidden')
                if features_div:
                    features_text = features_div.get_text(separator='\n', strip=True)
                    row["FEATURES"] = features_text
                    # print("-------------------------")
                    # print("FEATURES")
                    # print(row["FEATURES"])
                    # print("-------------------------")
                else:
                    raise ValueError("Couldn't find 'overflow-hidden' div.")
            else:
                raise ValueError("Couldn't find accordion content div.")
        except Exception as e:
            logging.warning(f"Features missing for {url}: {e}")


        # Images
        images = []
        thumbs = soup.find_all('img', class_="w-full block h-full absolute inset-0 square object-cover")
        if thumbs:
            for img in thumbs:
                src = img.get("src", "").replace("width=90", "width=1200")
                if "//www." in src:
                    images.append(src.replace("//", ""))
                else:
                    images.append(src)


        row["Images"] = list(set(images))
        # print(row["Images"])
        # Dimensions
        try:
            dim_table = soup.find('table', class_='table w-full border border-[#eaeaea]')
            trs = dim_table.find_all('tr')
            
            row["WEIGHT & DIMENSIONS"] = []
            for tr in trs:
                tds = tr.find_all('td')
                if len(tds) == 2:
                    label = tds[0].text.strip()
                    value = tds[1].text.strip()
                    if label!= "Assembly Instructions":
                        row["WEIGHT & DIMENSIONS"].append({label: value})
                    # Save overall dimensions in a single field if needed
                    if "Overall Width" in label:
                        row["Overall Dimensions"] = value  # or collect width, height, depth and format

                # print(row["WEIGHT & DIMENSIONS"])
        except Exception as e:
            logging.warning(f"Dimensions missing or invalid for {url}: {e}")


        # Assembly Info
        try:
            for tr in trs:
                tds = tr.find_all('td')
                if len(tds) == 2 and "Assembly Instructions" in tds[0].text:
                    link = tds[1].find('a')
                    if link:
                        row['Assembly Information'] = link['href']

                        # print(row['Assembly Information'])
        except Exception as e:
            logging.warning(f"Assembly info missing for {url}: {e}")


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
            product_links, current = await get_product_links(page, category_url[1], current=category_url[0])
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
