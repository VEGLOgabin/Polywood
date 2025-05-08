
import threading
import traceback,time
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import scrapy,os,json,datetime
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scrapy.crawler import CrawlerProcess



class AcsSpider(scrapy.Spider):
    name = "polywood"
    SOURCE_SITE='https://www.polywood.com'
    DATA=[]
    custom_settings = {
        "DOWNLOAD_DELAY": "2",
        "CONCURRENT_REQUESTS":"1",
        "ROBOTSTXT_OBEY":False,
        "COOKIES_ENABLED":False,
        "DOWNLOAD_TIMEOUT":600,
        "RETRY_ENABLED":True,
        "RETRY_TIMES":"6"

        }
    def restart(self):
        return Driver(uc=True,page_load_strategy='eager',headless=False)

    # driver2=restart()
    # driver3=restart()
    FILENAME= "output/output.json"
    def start_requests(self):
        yield scrapy.Request(
            url=self.SOURCE_SITE,
            callback=self.get_categ_links,

        )

    def get_categ_links(self, response):
        # print("****************")
        # print(response)
        # print("----------------")
        soup=BeautifulSoup(response.body,'html.parser')
        # print(soup.title)

        anchors=soup.find_all('a', class_ = "peer pb-sm -mb-sm block")
        # a_list = [item.get("href") for item in anchors]
        # print(a_list)

        for anchor in anchors:
            try:
                if anchor['href']:
                    if '/pages/' in anchor['href'] or '/collections/' in anchor['href']:
                        if anchor['href']!='https://www.polywood.com/styles/quick-ship-products.html':

                            yield scrapy.Request(
                                url= "https://www.polywood.com" + anchor['href'],
                                callback=self.get_products_links,

                            )
            except:
                pass
    def get_products_links(self, response):
        if str(response.status).strip()!='404':
            soup=BeautifulSoup(response.body,'html.parser')
            try:
                products=soup.find_all('a',class_='product-item-link')
                lnks=[]
                if '/collections/' in response.request.url:
                    current=[]
                    current.append(soup.find('h1').text.replace('Collection',''))
                else:
                    current=[]
                    lis=soup.find('ul',class_='items').find_all('li',recursive=False)
                    current.append(lis[-2].text.strip())
                    current.append(lis[-1].text.strip())

                for product in products:
                    try:
                        lnk=product['href']
                        lnks.append(lnk)
                    except:
                        pass
                self.get_products_details(lnks,current)
            except:
                pass

    def get_products_details(self,lnks,current):
        def scrape(driver,lnks,current):

            for lnk in lnks:
                try:
                    print('Scraping --> '+lnk)
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
                    soup=BeautifulSoup(driver.page_source,'html.parser')
                    row={}
                    row['Product Link']=lnk
                    row['Title']=soup.find('h1').text.strip()
                    row['SKU']=soup.find('div',attrs={'itemprop':'sku'}).text.strip()
                    if len(current)==1: row['Collection']=current[0].upper()
                    else:
                        row['Main Category']=current[0]
                        row['Collection']=current[1]
                    try:
                        row['Overview']=soup.find(class_='product attribute overview').text.strip()
                    except:
                        pass
                    try:
                        dsc=''
                        for li in soup.find(class_='product-info-feature-pillars').find_all('li'):
                            dsc+=li.text.strip()
                            dsc+='\n'

                        row['Description']=dsc.strip()
                    except:
                        pass
                    try:
                        dsc=''
                        for li in soup.find(class_='features').find_all('li'):
                            dsc+=li.text.strip()
                            dsc+='\n'

                        row['Features'.upper()]=dsc.strip()
                    except:
                        pass
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
                        dim_main=soup.find(class_='dimensions one two weight-dimensions')
                        row['Overall Dimensions']=dim_main.find('p').text.split(':')[1].strip()
                        trs=dim_main.find_all('tr')
                        row['Weight & Dimensions'.upper()]=[]
                        for tr in trs:
                            r={}
                            r[tr.find('td').text.strip()]=tr.find_all('td')[1].text.strip()
                            row['Weight & Dimensions'.upper()].append(r)
                    except:
                        try:
                            dim_main=soup.find(class_='dimensions one-only')
                            trs=dim_main.find_all('tr')
                            row['Weight & Dimensions'.upper()]=[]
                            for tr in trs:
                                r={}
                                r[tr.find('td').text.strip()]=tr.find_all('td')[1].text.strip()
                                row['Weight & Dimensions'.upper()].append(r)
                            pass
                        except:
                            pass
                    try:
                        for div in soup.find(class_='links').find_all('div'):
                            if 'Assembly Information'.lower() in div.text.lower():
                                row['Assembly Information']=self.SOURCE_SITE+div.find('a')['href']
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
                    df=pd.DataFrame(self.DATA)
                    # try:
                    #     df.sort_values(by=['Main Category','Collection','Collection 2'],inplace=True)
                    # except:
                    #     try:
                    #         df.sort_values(by=['Main Category','Collection'],inplace=True)
                    #     except:
                    #         try:
                    #             df.sort_values(by=['Main Category'],inplace=True)
                    #         except:
                    #             try:
                    #                 df.sort_values(by=['Collection'],inplace=True)
                    #             except:
                    #                 pass

                    rows_=df.to_dict('records')

                    rows_updates=[]
                    for row_ in rows_:
                        r={}
                        for key,value in row_.items():
                            if str(value).lower().strip()=='nan' or str(value).lower().strip()=='' or str(value).lower().strip()=='nat':
                                pass
                            else:
                                if not 'Link' in key:
                                    try:
                                        if '"' in value:
                                            value=value.replace('"','\"')
                                    except:
                                        pass
                                r[key]=value
                        rows_updates.append(r)

                    self.DATA=rows_updates
                    with open(self.FILENAME, 'w',encoding='utf8') as fout:
                        json.dump(self.DATA , fout,indent=4,ensure_ascii=False)
                except:
                    print('==================================================================')
                    print('==================================================================')
                    print('==================================================================')
                    print(lnk)
                    traceback.print_exc()
                    print('==================================================================')
                    print('==================================================================')
                    print('==================================================================')

        # lnks1=lnks[0:len(lnks)//3]
        lnks1=lnks
        # lnks2=lnks[len(lnks)//3:2*len(lnks)//3]
        # lnks3=lnks[2*len(lnks)//3:]
        driver1=self.restart()
        s1=threading.Thread(target=scrape,args=(driver1,lnks1,current,))
        # s2=threading.Thread(target=scrape,args=(self.driver2,lnks2,current,))
        # s3=threading.Thread(target=scrape,args=(self.driver3,lnks3,current,))
        s1.start()
        # s2.start()
        # s3.start()
        s1.join()
        # s2.join()
        # s3.join()
    try:
        driver1.quit()
    except:
        pass
    # try:
    #     self.driver2.quit()
    # except:
    #     pass
    # try:
    #     self.driver3



def run_spiders():
    process = CrawlerProcess()
    process.crawl(AcsSpider)
    process.start()

run_spiders()