import re
import json
import threading
from queue import Queue
from datetime import date
from typing import Optional
from dataclasses import dataclass

import requests
import pandas as pd
from requests import Response
from bs4 import BeautifulSoup

from utils import Logger

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

PARAMS = {"pageNumber": "1", "keyword": ""}

EQUIPEMENT = dict[str, str|dict[str, str|list[dict[str, str]]]]

OUTPUT_PATH = "./data/"

@dataclass
class Equipement:
    data_dict: EQUIPEMENT

    def __post_init__(self) -> None:
        self.price = self.data_dict["price"]
        self.date = self.data_dict["date"]
        self.link = self.data_dict["link"]

        try:
            self.year = self.data_dict["modelYear"]["values"][0]["textSpans"][0]["text"]
        except:
            try:
                self.year = re.search(r"\d{4,}", self.data_dict["desc"]).group()
            except:
                self.year = ""
        
        index = 1 if self.year.strip() else 0
        
        try:
            self.make = self.data_dict["make"]["values"][0]["textSpans"][0]["text"]
        except:
            self.make = self.data_dict["desc"].split(" ")[index]
        
        try:
            self.model = self.data_dict["model"]["values"][0]["textSpans"][0]["text"]
        except:
            self.model = self.data_dict["desc"].split(" ")[index + 1]

        try:
            self.hours = self.data_dict["hours"]["values"][0]["textSpans"][0]["text"]
        except:
            self.hours = ""

    def __repr__(self) -> str:
        return str(f"Equipement(year={self.year}, make={self.make}, model={self.model}, "
                   f"hours={self.hours}, bidadoo_price={self.price}, "
                   f"sale_date={self.date})")

class BidadooScraper:
    """Scrapes equipements https://www.bidadoo.com/results"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*****Bidadoo Scraper Started*****")

        self.crawled = []
        self.equipments = []

        self.queue = Queue()
        self.regex = re.compile(r"\[\]\)\.concat\((.*)\)</script></body>")

        self.thread_num = 10
        self.base_url = "https://www.bidadoo.com/results"
    
    def __fetch_page(self, url: str, params: Optional[dict[str, str]]=None) -> Response:
        """Fetches a webpage from a given link"""
        for _ in range(10):
            try:
                response = requests.get(url, params=params, headers=HEADERS, timeout=10)

                if response.ok:
                    return response
            
            except:pass

            self.logger.warn("Couldn't retrieve item from {}. Retrying...".format(url))

    def __extract_bidadoo_items(self, response: Response) -> list[dict[str, str]]:
        """Extracts bidadoo equipements from the response"""
        soup = BeautifulSoup(response.text, "html.parser")

        results = soup.find("div", {"class": "results"})

        equipements = []

        for equipement in results.select("div.category__ct"):
            price = equipement.select_one("div.category__txt > p")
            link_tag = equipement.select_one("a.category__butt")
            description = equipement.select_one("div.category__head")

            equipements.append({"desc": description.get_text(strip=True),
                                "price": price.get_text(strip=True),
                                "link": link_tag["href"],
                                "date": link_tag.get_text(strip=True).split(" ")[-1]})
        
        self.logger.info("Equipements found: {}".format(len(equipements)))

        return equipements

    def __extract_ebay_slugs(self, response: Response) -> EQUIPEMENT:
        """Extract ebay slugs from the response object"""
        json_str = self.regex.search(response.text).group(1)

        json_data = json.loads(json_str)["o"]["w"][0][2]["model"]["modules"]

        return json_data["ABOUT_THIS_ITEM"]["sections"]["features"]["dataItems"]

    def __work(self) -> None:
        """Work to be done by threads"""
        while True:
            item = self.queue.get()

            # if item["link"] in self.crawled:
            #     self.queue.task_done()

            #     continue

            try:
                response = self.__fetch_page(item["link"])

                ebay_slugs = self.__extract_ebay_slugs(response)

                equipement = Equipement({**ebay_slugs, **item})

                self.page_results.append({"YEAR": equipement.year,
                                          "MAKE": equipement.make,
                                          "MODEL": equipement.model, 
                                          "HOURS": equipement.hours, 
                                          "BIDADOO PRICE": equipement.price,
                                          "SALE DATE": equipement.date, 
                                          "PREVIOUS  OWNER": "",
                                          "LINK TO LISTING": equipement.link})
                
                self.queue_len -= 1

                self.crawled.append(equipement.link)

                args = (self.queue_len, len(self.crawled))
                
                self.logger.info("Queue: {} || Crawled: {}".format(*args))
            
            except: 
                try:
                    try:
                        year = re.search(r"\d{4,}", item["desc"]).group()
                    except:
                        year = ""

                    self.page_results.append({"YEAR": year,
                                              "MAKE": item["desc"].split(" ")[1],
                                              "MODEL": item["desc"].split(" ")[2], 
                                              "HOURS": "", 
                                              "BIDADOO PRICE": item["price"],
                                              "SALE DATE": item["date"], 
                                              "PREVIOUS  OWNER": "",
                                              "LINK TO LISTING": item["link"]})
                except:
                    self.logger.error()

                    with open("error.json", "w") as f:
                        json.dump(item, f, indent=4)
            
            self.queue.task_done()

    def __save_to_csv(self) -> None:
        """Save data retrieved to csv"""
        self.logger.info("Saving data retrieved to excel...")

        df = pd.DataFrame(self.equipments).drop_duplicates()

        filename = f"results_{date.today()}.xlsx"

        df.to_excel(f"{OUTPUT_PATH}{filename}", index=False)

        self.logger.info("{} records saved to {}".format(len(df), filename))

    def scrape(self) -> None:
        """Entry point to the scraper"""
        page = 1
        [threading.Thread(target=self.__work, 
                          daemon=True).start() for _ in range(self.thread_num)]

        while True:
            self.logger.info("Fetching equipements from page: {}".format(page))

            self.page_results = []

            PARAMS["pageNumber"] = str(page)

            params = PARAMS

            if page == 1:
                params = None

            response = self.__fetch_page(self.base_url, params=params)

            equipements = self.__extract_bidadoo_items(response)

            self.queue_len = len(equipements)

            [self.queue.put(equipement) for equipement in equipements]
            self.queue.join()

            for item in equipements:
                for equipment in self.page_results:
                    if item["link"] == equipment["LINK TO LISTING"] \
                        and item["price"] == equipment["BIDADOO PRICE"] \
                            and item["date"] == equipment["SALE DATE"]:
                        self.equipments.append(equipment)

            soup = BeautifulSoup(response.text, "html.parser")

            total_pages = soup.select_one("div.results")["data-num-pages"]
            
            self.__save_to_csv()

            if page >= int(total_pages):
                break

            page += 1

if __name__ == "__main__":
    scraper = BidadooScraper()
    scraper.scrape()