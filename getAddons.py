import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options 

import threading
from concurrent.futures.thread import ThreadPoolExecutor
from tinydb import TinyDB, Query

DB_PATH = './db.json'
LOCK = threading.Lock()

chrome_options = Options()  
chrome_options.add_argument("--headless")  
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--no-sandbox')

URLs1 = [each[:each.index('?')] for each in open('category_addons.txt', 'r').readlines()]
URLs2 = [each.strip() for each in open('collectURL.txt', 'r').readlines()]
uniqueURLs = set(URLs1) | set(URLs2)

db = TinyDB(DB_PATH)
crawled = [r['url'] for r in db.table('_default').all()]
toCrawl = list([url for url in uniqueURLs if url not in crawled])

def feth_crawlURLs(url, driver_index, LOCK):
    global crawled, toCrawl, drivers
    driver = drivers[driver_index]['driver']
    driver.get(url)
    
    html = driver.find_element_by_tag_name('html')
    #html.text 
    
    urls = [each.get_attribute('href') for each in html.find_elements_by_tag_name('a')]
    urls = list(filter(lambda x: x is not None and 'webstore/detail/' in x, urls))
    summary = driver.find_element_by_class_name('C-b-p-j').text
    stats = driver.find_element_by_class_name('e-f-w-Va').text
    with LOCK:
        drivers[driver_index]['available'] = True

        db = TinyDB('./db.json')
        db.insert({'url': url, 
                   'summary': summary,
                   'stats': stats})

        crawled.append(url)
        urls = list(filter(lambda x: x not in crawled, urls))
        
        for url in urls:
            db.table('newURLs').insert({'url': url})
        
        toCrawl += urls
    return None

MAX_WORKERS = 30
drivers = [{'driver': webdriver.Chrome(options=chrome_options),
            'available': True } for i in range(MAX_WORKERS)]

if __name__ == '__main__':    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while toCrawl:      
            with LOCK:
                driver_index = None
                for index, driver in enumerate(drivers):
                    if driver['available'] == True:
                        driver['available'] = False
                        driver_index = index
                        break

            if driver_index is not None:             
                executor.submit(feth_crawlURLs, toCrawl.pop(0), driver_index, LOCK)
                with LOCK:
                    print('Crawled: {:10d} To Crawl: {:10d}'.format(len(crawled), 
                                                len(toCrawl)), end='\r')
            elif driver_index is None:
                time.sleep(0.2);

        print('Nothing to DO')
