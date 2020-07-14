"""
Extracts contents from Odia Wikipedia
Written by: Soumendra Kumar Sahoo
Start date: 14 May 2020

Inspired from: https://github.com/goru001/nlp-for-odia
Reference: https://towardsdatascience.com/5-strategies-to-write-unblock-able-web-scrapers-in-python-5e40c147bdaf
"""
import asyncio
import pickle
import random
from typing import Dict

import aiofiles as aiofiles
import aiohttp
import os
import re
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import Retry

# SESSION = requests.session()
# RETRIES = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 501, 502, 503, 504])
# SESSION.mount("https://", HTTPAdapter(max_retries=RETRIES))
HOME_URL = "https://or.wikipedia.org"
OUTPUT_PATH = os.path.join(os.getcwd(), "monolingual/raw/data/")
ALL_LINKS_PICKLE_PATH = os.path.join(OUTPUT_PATH, "all_links.pkl")
PARSER = "html.parser"
ALL_LINKS = dict()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Pragma": "no-cache",
    "referer": "https://or.wikipedia.org/wiki/%E0%AC%AA%E0%AD%8D%E0%AC%B0%E0%AC%A7%E0%AC%BE%E0%AC%A8_"
    "%E0%AC%AA%E0%AD%83%E0%AC%B7%E0%AD%8D%E0%AC%A0%E0%AC%BE",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
}


async def fetch_article_header_links():
    """
    Fetches all the 52 alphabets links from the Wikipedia
    :return: Dictionary of links with its title as values
    :rtype: Dict[str, str]
    """
    # if os.path.exists(ALL_LINKS_PICKLE_PATH):
    #     return None
    async with aiohttp.ClientSession().get("https://or.wikipedia.org", headers=HEADERS) as session:
        html = await session.read()
        soup = BeautifulSoup(html, PARSER)
        tab = soup.find(
            "table",
            {"style": "border:2px solid #e1eaee; border-collapse:separate;font-size:120%"},
        )
        anchors = tab.find_all("a")
        links = [HOME_URL + anchor["href"] async for anchor in anchors]
        return links


async def fetch_article_links(link):
    async with aiohttp.ClientSession().get(link, headers=HEADERS) as link_response:
        html = await link_response.read()
    soup = BeautifulSoup(html, PARSER)
    div = soup.find("div", {"class": "mw-allpages-body"})
    if div:
        anchors = div.find_all("a")
        async for anchor in anchors:
            ALL_LINKS[anchor.text] = HOME_URL + anchor["href"]


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
        try:
            async with aiofiles.open(filename, "w+") as output_file:
                print(f"Writing into file: {filename}")
                await output_file.write(article)
        except FileNotFoundError as error:
            print(f"Unable to write the file: {filename} due to: {error}")
        return await link_response.release()


async def processor(all_links, title):
    """
    Main processor
    :param all_links:
    :type all_links:
    :param title:
    :type title:
    :return:
    :rtype:
    """
    url = all_links.get(title)
    async with aiohttp.ClientSession() as session:
        filename = os.path.join(OUTPUT_PATH, title + ".txt")
        delays = [2, 5, 1, 7, 4, 9]
        print(f"Fetching the article: {title} with URL: {url}")
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
    article_text = article_text.replace("|", "।")
    return article_text


async def main():
    loop = asyncio.get_event_loop()
    task1 = await asyncio.create_task(fetch_article_header_links())
    links = task1.result()
    async for link in links:
        await fetch_article_links(link)
    loop.run_until_complete(asyncio.gather(*(fetch_article_links(link) for link in links)))
    async with aiofiles.open(ALL_LINKS_PICKLE_PATH, "rb") as pf:
        all_urls = pickle.load(pf)
    print(f"The number of URLs fetched are: {len(all_urls)}")
    loop = asyncio.get_running_loop()
    loop.run_until_complete(asyncio.gather(*(processor(all_urls, title) for title in all_urls)))
    loop.close()
