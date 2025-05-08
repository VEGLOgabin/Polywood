import threading
import traceback
import time
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import scrapy
import os
import json
import datetime
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from scrapy.crawler import CrawlerProcess


class AcsSpider(scrapy.Spider):
    name = "polywood"
    SOURCE_SITE = 'https://www.polywood.com'
    DATA = []
    custom_settings = {
        "DOWNLOAD_DELAY": "2",
        "CONCURRENT_REQUESTS": "1",
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": False,
        "DOWNLOAD_TIMEOUT": 600,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": "6"
    }

    FILENAME = "output/output.json"

    def restart(self):
        return Driver(uc=True, page_load_strategy='eager', do_not_track=True, block_images=True, ad_block=True, headless=False)

    def load_existing_data(self):
        """Load existing data from the output file to avoid re-scraping."""
        if os.path.exists(self.FILENAME):
            with open(self.FILENAME, 'r', encoding='utf8') as file:
                self.DATA = json.load(file)
                print(f"Loaded {len(self.DATA)} existing entries from {self.FILENAME}.")
        else:
            print(f"No existing data file found at {self.FILENAME}.")

    def start_requests(self):
        self.load_existing_data()
        yield scrapy.Request(
            url=self.SOURCE_SITE,
            callback=self.get_categ_links,
        )

    def get_categ_links(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')
        anchors = soup.find_all('a')
        for anchor in anchors:
            try:
                if anchor['href'].endswith('.html'):
                    if '/styles/' in anchor['href'] or '/collections/' in anchor['href']:
                        if anchor['href'] != 'https://www.polywood.com/styles/quick-ship-products.html':
                            yield scrapy.Request(
                                url=anchor['href'] + '?&sp=1500',
                                callback=self.get_products_links,
                            )
            except:
                pass

    def get_products_links(self, response):
        if str(response.status).strip() != '404':
            soup = BeautifulSoup(response.body, 'html.parser')
            try:
                products = soup.find_all('a', class_='product-item-link')
                lnks = []
                if '/collections/' in response.request.url:
                    current = [soup.find('h1').text.replace('Collection', '')]
                else:
                    current = []
                    lis = soup.find('ul', class_='items').find_all('li', recursive=False)
                    current.append(lis[-2].text.strip())
                    current.append(lis[-1].text.strip())

                for product in products:
                    try:
                        lnk = product['href']
                        lnks.append(lnk)
                    except:
                        pass
                self.get_products_details(lnks, current)
            except:
                pass

    def get_products_details(self, lnks, current):
        def scrape(driver, lnks, current):
            scraped_links = {entry['Product Link'] for entry in self.DATA}

            for lnk in lnks:
                if lnk in scraped_links:
                    print(f"Skipping already scraped link: {lnk}")
                    continue

                try:
                    print('Scraping --> ' + lnk)
                    driver.get(lnk)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//div[@class="gallery-placeholder"]//img'))
                    )
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located(
                                (By.XPATH, '//div[@data-gallery-type="thumbnail"]//img'))
                        )
                    except:
                        pass
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    row = {}
                    row['Product Link'] = lnk
                    row['Title'] = soup.find('h1').text.strip()
                    row['SKU'] = soup.find('div', attrs={'itemprop': 'sku'}).text.strip()
                    if len(current) == 1:
                        row['Collection'] = current[0].upper()
                    else:
                        row['Main Category'] = current[0]
                        row['Collection'] = current[1]
                    try:
                        row['Overview'] = soup.find(class_='product attribute overview').text.strip()
                    except:
                        pass
                    try:
                        dsc = ''
                        for li in soup.find(class_='product-info-feature-pillars').find_all('li'):
                            dsc += li.text.strip() + '\n'
                        row['Description'] = dsc.strip()
                    except:
                        pass
                    try:
                        dsc = ''
                        for li in soup.find(class_='features').find_all('li'):
                            dsc += li.text.strip() + '\n'
                        row['Features'.upper()] = dsc.strip()
                    except:
                        pass
                    # try:
                    #     row['Images'] = list(set(
                    #         div['src'].replace('w_200,h_160,c_fill,q_80', 'w_700,h_700,c_pad,q_80')
                    #         for div in soup.find('div', attrs={'data-gallery-type': 'thumbnail'}).find_all('img')
                    #     ))
                    # except:
                    #     row['Images'] = []

                    try:
                        i=0
                        row['Images']=[]
                        for div in soup.find('div',attrs={'data-gallery-type':'thumbnail'}).find_all('img'):
                            try:
                                i+=1
                                rw=div['src'].replace('w_200,h_160,c_fill,q_80','w_700,h_700,c_pad,q_80').\
                                    replace('w_100,h_80,c_fill,q_80','w_700,h_700,c_pad,q_80')
                                row['Images'].append(rw)
                            except:
                                traceback.print_exc()
                                pass
                        if len(row['Images'])==0:
                            driver.find_element(By.XPATH,'///////')
                    except:
                        i=0
                        row['Images']=[]
                        for div in soup.find('div',class_='gallery-placeholder').find_all('img'):
                            try:
                                i+=1
                                rw=div['src']
                                row['Images'].append(rw)
                            except:
                                traceback.print_exc()
                                pass
                    try:
                        dim_main = soup.find(class_='dimensions one two weight-dimensions')
                        row['Overall Dimensions'] = dim_main.find('p').text.split(':')[1].strip()
                        trs = dim_main.find_all('tr')
                        row['Weight & Dimensions'.upper()] = [
                            {tr.find('td').text.strip(): tr.find_all('td')[1].text.strip()} for tr in trs
                        ]
                    except:
                        pass
                    try:
                        for div in soup.find(class_='links').find_all('div'):
                            if 'Assembly Information'.lower() in div.text.lower():
                                row['Assembly Information'] = self.SOURCE_SITE + div.find('a')['href']
                                break
                    except:
                        pass


                    try:
                        row['SKU Options'] = []
                        option_group = soup.find('div', class_='option-groupings')
                        if option_group:
                            options = option_group.find_all('div', class_='grouping-option-value')
                            for option in options:
                                sku_option = {
                                    "SKU": option.get("option-sku", "").strip(),
                                    "Color": option.get("option-label", "").strip()
                                }
                                row['SKU Options'].append(sku_option)
                    except Exception as e:
                        print(f"Error while extracting SKU options: {e}")

                    row['Images'] = list(set(row['Images']))
                    time.sleep(2)
 

                    self.DATA.append(row)
                    with open(self.FILENAME, 'w', encoding='utf8') as fout:
                        json.dump(self.DATA, fout, indent=4, ensure_ascii=False)
                except:
                    traceback.print_exc()

        driver1 = self.restart()
        s1 = threading.Thread(target=scrape, args=(driver1, lnks, current,))
        s1.start()
        s1.join()

        try:
            driver1.quit()
        except:
            pass


def run_spiders():
    process = CrawlerProcess()
    process.crawl(AcsSpider)
    process.start()


run_spiders()
