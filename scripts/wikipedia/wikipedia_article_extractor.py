"""
Extracts contents from Odia Wikipedia
Written by: Soumendra Kumar Sahoo
Start date: 14 May 2020

Inspired from: https://github.com/goru001/nlp-for-odia
Reference: https://towardsdatascience.com/5-strategies-to-write-unblock-able-web-scrapers-in-python-5e40c147bdaf
"""
import asyncio
import random
from typing import Dict

import aiofiles as aiofiles
import aiohttp
import os
import re
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

SESSION = requests.session()
RETRIES = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 501, 502, 503, 504])
SESSION.mount("https://", HTTPAdapter(max_retries=RETRIES))
OUTPUT_PATH = os.path.join(os.getcwd(), "monolingual/raw/wikipedia/")
PARSER = "html.parser"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Pragma": "no-cache",
    "referer": "https://or.wikipedia.org/wiki/%E0%AC%AA%E0%AD%8D%E0%AC%B0%E0%AC%A7%E0%AC%BE%E0%AC%A8_"
    "%E0%AC%AA%E0%AD%83%E0%AC%B7%E0%AD%8D%E0%AC%A0%E0%AC%BE",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
}


def fetch_article_links() -> Dict[str, str]:
    """
    Fetches all the 52 alphabets links from the Wikipedia
    :return: Dictionary of links with its title as values
    :rtype: Dict[str, str]
    """
    with SESSION:
        response = SESSION.get("https://or.wikipedia.org", headers=HEADERS)
    soup = BeautifulSoup(response.text, PARSER)
    tab = soup.find(
        "table",
        {"style": "border:2px solid #e1eaee; border-collapse:separate;font-size:120%"},
    )
    anchors = tab.find_all("a")
    home_url = "https://or.wikipedia.org"
    links = [home_url + anchor["href"] for anchor in anchors][:1]
    prev_len = 0
    with SESSION:
        for link in tqdm(links):
            while link:
                response = SESSION.get(link)
                soup = BeautifulSoup(response.text, PARSER)
                div = soup.find("div", {"class": "mw-allpages-body"})
                if div:
                    anchors = div.find_all("a")
                    all_links = {
                        anchor.text: home_url + anchor["href"] for anchor in anchors
                    }
                if prev_len == len(all_links):
                    break
                nav_div = soup.find("div", {"class": "mw-allpages-nav"})
                if nav_div and len(nav_div.find_all("a")) == 2:
                    link = home_url + nav_div.find_all("a")[1]["href"]
                prev_len = len(all_links)
    return all_links


async def write_link_text(url, filename, session) -> None:
    """
    Extracts data from the links and write into the files
    :param url:
    :type url:
    :param filename:
    :type filename:
    :param session:
    :type session:
    :return:
    :rtype:
    """
    async with session.get(url, headers=HEADERS) as link_response:
        html = await link_response.read()
        link_soup = BeautifulSoup(html, PARSER)
        paras = link_soup.find_all("p")
        article = "\n".join([para.text for para in paras])
        article = await process_text(article)
        async with aiofiles.open(filename, "w+") as output_file:
            print(f"Writing into file: {filename}")
            await output_file.write(article)
        return await link_response.release()


async def main(title, url):
    """
    Main processor
    :param title:
    :type title:
    :param url:
    :type url:
    :return:
    :rtype:
    """
    async with aiohttp.ClientSession() as session:
        filename = OUTPUT_PATH + title + ".txt"
        print(f"Fetching the article: {title} with URL: {url}")
        delays = [2, 5, 1, 7, 4, 9]
        await asyncio.sleep(random.choice(delays))
        await write_link_text(url, filename, session)


async def process_text(article_text: str) -> str:
    """
    Process the text assigned to it
    :param article_text:
    :type article_text:
    :return: article_text
    :rtype: str
    """
    article_text = re.sub(r"\([^)]*\)", r"", article_text)
    article_text = re.sub(r"\[[^\]]*\]", r"", article_text)
    article_text = re.sub(r"<[^>]*>", r"", article_text)
    article_text = re.sub(r"^https?:\/\/.*[\r\n]*", "", article_text)
    article_text = article_text.replace("\ufeff", "")
    article_text = article_text.replace("\xa0", " ")
    article_text = article_text.replace("  ", " ")
    article_text = article_text.replace(" , ", ", ")
    return article_text


if __name__ == "__main__":
    all_links = fetch_article_links()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*(main(title, url) for title, url in all_links)))
