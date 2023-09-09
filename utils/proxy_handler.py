import time
import threading
from queue import Queue

import requests
from bs4 import BeautifulSoup

from .logger import Logger


class ProxyHandler:
    def __init__(self, bad_proxies: list) -> None:
        self.ports = ["3128", "3124", "80", "8080"]
        self.proxies = []
        self.bad_proxies = bad_proxies
        
        self.proxy_queue = Queue()
        
        self.logger = Logger("ProxyHandler")

    def get_proxies(self) -> None:
        """Fetches proxies from https://free-proxy-list.net/"""

        while True:
            try:
                url = 'https://free-proxy-list.net/'
                response = requests.get(url)
                proxies_table = BeautifulSoup(response.text, "html.parser")

                if response.status_code != 200:
                    continue

                table_rows = proxies_table.select("tbody tr")

                if not len(table_rows):
                    continue

                for row in table_rows[:299]:
                    for port in self.ports:   
                        proxy = ":".join(
                            [row.select('td')[0].text.strip(), 
                             row.select('td')[1].text.strip()])

                        if proxy not in self.bad_proxies \
                            and proxy not in self.proxies:
                
                            self.proxies.append(proxy)

                url = "https://sslproxies.org/"

                response = requests.get(url)
                proxies_table = BeautifulSoup(response.text, "html.parser")

                if response.status_code != 200:
                    continue

                table_rows = proxies_table.select("tbody tr")

                if not len(table_rows):
                    continue

                
                for row in table_rows:
                    try:
                        cells = row.select("td")

                        ip = cells[0].get_text(strip=True)
                        port = cells[1].get_text(strip=True)

                        if not f"{ip}:{port}" in self.bad_proxies \
                            and f"{ip}:{port}" not in self.proxies:
                            self.proxies.append(f"{ip}:{port}")
                    except:pass

            except Exception as e: print(e)

            self.proxies = list(set(self.proxies))

            bad_proxies = [*self.bad_proxies]

            [self.proxies.remove(proxy) for proxy in bad_proxies if proxy in self.proxies]

            self.logger.info(f"Proxies found: {len(self.proxies)}")

            time.sleep(10)