"""
Extracts contents from Odia Wikipedia
Written by: Soumendra Kumar Sahoo
Start date: 14 May 2020

Inspired from: https://github.com/goru001/nlp-for-odia
Reference: https://towardsdatascience.com/5-strategies-to-write-unblock-able-web-scrapers-in-python-5e40c147bdaf
"""
import asyncio
from collections import ChainMap
import concurrent
import json
import os
import pickle
import random
import re

from aiohttp.client import ClientTimeout
from scripts.utility import USER_AGENTS_LIST
from typing import Dict, List

import aiofiles as aiofiles
import aiohttp
from bs4 import BeautifulSoup


HOME_URL = "https://or.wikipedia.org"
ALL_LINKS_PICKLE_PATH = os.path.join(os.getcwd(), "data/links/all_links.json")
OUTPUT_DIR = os.path.join(os.getcwd(), "data/articles/")
PARSER = "html.parser"
HEADERS = {
    "User-Agent": random.choice(USER_AGENTS_LIST),
    "Pragma": "no-cache",
    "referer": "https://or.wikipedia.org/wiki/%E0%AC%AA%E0%AD%8D%E0%AC%B0%E0%AC%A7%E0%AC%BE%E0%AC%A8_"
    "%E0%AC%AA%E0%AD%83%E0%AC%B7%E0%AD%8D%E0%AC%A0%E0%AC%BE",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
}
POOL = concurrent.futures.ProcessPoolExecutor()


async def fetch_article_header_links(session, loop):
    """
    Fetches all the 52 alphabets links from the Wikipedia
    :return: Dictionary of links with its title as values
    """
    async with session.get("https://or.wikipedia.org", headers=HEADERS) as response:
        return await response.text()


def get_links(html: str) -> List:
    """ Get link with the help of blocking Beautifulsoup """
    soup = BeautifulSoup(html, PARSER)
    tab = soup.find(
        "table",
        {"style": "border:2px solid #e1eaee; border-collapse:separate;font-size:120%"},
    )
    anchors = tab.find_all("a")
    links = [HOME_URL + anchor["href"] for anchor in anchors]
    return links


async def fetch_article_links(session, link) -> dict:
    async with session.get(link, headers=HEADERS) as link_response:
        html = await link_response.text()
    return get_all_links(html)


def get_all_links(html: str) -> dict:
    all_links = dict()
    soup = BeautifulSoup(html, PARSER)
    div = soup.find("div", {"class": "mw-allpages-body"})
    if div:
        anchors = div.find_all("a")
        for anchor in anchors:
            all_links[anchor.text] = HOME_URL + anchor["href"]
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
        html = await link_response.text()
        article = get_article(html)
        # article = await loop.run_in_executor(POOL, get_article, html)
        try:
            async with aiofiles.open(filename, "w+") as output_file:
                print(f"Writing into file: {os.path.basename(filename)} : {len(article)}")
                await output_file.write(article)
        except FileNotFoundError as error:
            print(f"Unable to write the file: {filename} due to: {error}")
        return await link_response.release()


def get_article(html: str):
    link_soup = BeautifulSoup(html, PARSER)
    paras = link_soup.find_all("p")
    article = "\n".join([para.text for para in paras])
    article = process_text(article)


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
    try:
        url = all_links.get(title)
        title = title.replace('/', '-')
        async with aiohttp.ClientSession() as session:
            filename = f"{OUTPUT_DIR}{title}.txt"
            print(f"Fetching the article: {title} with URL: {url}")
            sleep_time = random.randrange(20)
            print(f"sleeping for {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)
            await write_link_text(url, filename, session)
    except Exception as e:
        print(f"got error: {e} while running processor.")


def process_text(article_text: str) -> str:
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
    try:
        loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10 * 60)) as session:
            if not os.path.exists(ALL_LINKS_PICKLE_PATH):
                html = await fetch_article_header_links(session, loop)
                links = await loop.run_in_executor(POOL, get_links, html)
                all_links = await asyncio.gather(
                    *(fetch_article_links(session, link) for link in links)
                )
                all_links = dict(ChainMap(*all_links))
                print(f"{len(all_links)} links found.")
                with open(ALL_LINKS_PICKLE_PATH, "w+", encoding="utf-8") as link_handler:
                    print(f"Writing into file: {ALL_LINKS_PICKLE_PATH}")
                    # pickle.dump(all_links, link_handler)
                    json.dump(all_links, link_handler, ensure_ascii=False, indent=4)
                    print(f"{len(all_links)} links written.")
            else:
                with open(ALL_LINKS_PICKLE_PATH) as ph:
                    all_links = json.load(ph)
                print(f"{len(all_links)} links found by reading the file.")
        loop = asyncio.get_running_loop()
        loop.run_until_complete(
            await asyncio.gather(
                *(processor(all_links, title) for title in all_links)
            )
        )
            # for title in all_links:
            #     await processor(session, loop, all_links, title)
        # loop.close()
    except Exception as e:
        print(f"failed with error: {e}")
