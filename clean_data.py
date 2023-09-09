import json
import random
import threading
from queue import Queue
from datetime import date
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests import Response
from fake_useragent import UserAgent

from utils import Logger, ProxyHandler

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Dnt": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-site",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
}

OUTPUT_PATH = "./cleaned/"

class CleanExcelData:
    """Cleans the data in excel"""
    requests.packages.urllib3.disable_warnings()

    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*****Data Cleaner Started*****")

        self.queue = Queue()
        self.thread_num = 100

        self.cleaned = []
        self.json_data = []
        self.bad_proxies = set()
        self.uncleaned = self.__read_excel()

    def __read_excel(self) -> pd.DataFrame:
        """Retrives uncleaned data from excel"""
        return pd.read_excel("./data/results_2023-09-08.xlsx")

    def __fetch_page(self, url: str) -> Response:
        """Retrieves a page from ebay"""
        while True:
            while not len(self.proxies):pass

            ip_prort = random.choice(self.proxies)

            proxy = {"https":f"http://{ip_prort}"}

            try:
                

                HEADERS["User-Agent"] = UserAgent().random.strip()

                response =  requests.get(
                    url, headers=HEADERS, timeout=10, proxies=proxy, verify=False)

                if response.ok or response.status_code == 404:
                    return response
                
                # print(response.status_code)

            except: 
                try:
                    if len(set(self.proxies)) > 20:
                        self.proxies.remove(ip_prort)

                        self.bad_proxies.add(ip_prort)

                except:pass

    def __get_iframe_source(self, response: Response) -> Optional[str]:
        """Gets an iframe source from the response object"""
        try:
            soup = BeautifulSoup(response.text, "html.parser")

            return soup.select_one("iframe#desc_ifr")["src"]
        
        except:pass

    def __extract_item_slugs(self, response: Response) -> Optional[dict[str, str]]:
        """Extracts item slugs from the response object"""
        equipement = {}
            
        try:
            soup = BeautifulSoup(response.text, "html.parser")

            container = soup.select_one("div.container")

            unordered_list = container.select_one("ul.list-group")

            for list_item in unordered_list.select("li"):
                try:
                    key, value = list_item.get_text(strip=True).split(":")

                    equipement[key.strip()] = value.strip()
                except:
                    pass
        except:pass
            
        return equipement

    def __create_work(self, excel_data: pd.DataFrame) -> None:
        """Creates work to be done by threads"""
        equipements = excel_data.to_dict("records")

        self.queue_len = len(equipements)

        [self.queue.put(item) for item in equipements]

        self.queue.join()

    def __work(self) -> None:
        """Work to be done by threads"""
        while True:
            item = self.queue.get()

            response = self.__fetch_page(item["LINK TO LISTING"])

            iframe_url = self.__get_iframe_source(response)

            if iframe_url is not None:
                response = self.__fetch_page(iframe_url)

                equipement = self.__extract_item_slugs(response)

                cleaned_item = {**item, 
                                "YEAR": equipement.get("Year", ""),
                                "MAKE": equipement.get("Make", ""),
                                "MODEL": equipement.get("Model", ""),
                                "HOURS": equipement.get("Hours", "")}
                
                if len(equipement):
                    self.cleaned.append(cleaned_item)
                else:
                    self.cleaned.append(item)

                    self.json_data.append(item)

                    with open("rejected.json", "w") as f:
                        json.dump(self.json_data, f, indent=False)

            else:
                self.cleaned.append(item)

            if len(self.cleaned) % 10 == 0:
                try:
                    self.__save_to_csv()
                except:pass

            args = (self.queue_len - len(self.cleaned), len(self.cleaned))

            self.logger.info("Queue: {} || Crawled: {}".format(*args))

            self.queue.task_done()
    
    def __order_results(self) -> list[dict[str, str]]:
        """Sorts the data in the correct order"""
        items = self.cleaned

        ordered_list = []

        for ordered_item in self.uncleaned_list:
            previous_item = (ordered_item["SALE DATE"], 
                             ordered_item["BIDADOO PRICE"],
                             ordered_item["LINK TO LISTING"])
            
            for unordered_item in items:
                if previous_item == (unordered_item["SALE DATE"],
                                     unordered_item["BIDADOO PRICE"],
                                     unordered_item["LINK TO LISTING"]):
                    ordered_list.append(unordered_item)

                    break
        
        return ordered_list

    def __save_to_csv(self) -> None:
        """Save items to csv"""
        self.logger.info("Saving data retrieved to excel...")

        ordered_data = self.__order_results()

        df = pd.DataFrame(ordered_data).drop_duplicates()

        filename = f"cleaned_data_{date.today()}.xlsx"

        df.to_excel(f"{OUTPUT_PATH}{filename}", index=False)

        self.logger.info("{} records saved to {}".format(len(df), filename))

    def run(self) -> None:
        """Entry point to the cleaner"""
        [threading.Thread(target=self.__work, 
                          daemon=True).start() for _ in range(self.thread_num)]
        
        proxy_handler = ProxyHandler(self.bad_proxies)

        [threading.Thread(target=proxy_handler.get_proxies, 
                          daemon=True).start() for _ in range(1)]
        
        self.proxies = proxy_handler.proxies

        while not len(self.proxies):pass
        
        self.uncleaned_list = self.uncleaned.to_dict("records")
        
        self.__create_work(self.uncleaned)

        self.__save_to_csv()

        self.logger.info("Finished!")


if __name__ == "__main__":
    cleaner = CleanExcelData()
    cleaner.run()