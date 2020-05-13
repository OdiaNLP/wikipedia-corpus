"""
Extracts contents from Odia Wikipedia
Written by: Soumendra Kumar Sahoo
Date: 14 May 2020

Inspired from: https://github.com/goru001/nlp-for-odia
"""
from typing import List

import os
import re
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from tqdm import tqdm

SESSION = requests.session()
RETRIES = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 501, 502, 503, 504])
SESSION.mount("https://", HTTPAdapter(max_retries=RETRIES))
OUTPUT_PATH = os.path.join(os.getcwd(), "monolingual/raw/wikipedia/")


def fetch_article_links() -> List[str]:
    """
    Fetches all the 52 alphabets links from the Wikipedia
    :return: List of links
    :rtype: List[str]
    """
    with SESSION:
        response = SESSION.get("https://or.wikipedia.org")
    soup = BeautifulSoup(response.text, "html.parser")
    tab = soup.find(
        "table",
        {"style": "border:2px solid #e1eaee; border-collapse:separate;font-size:120%"},
    )
    anchors = tab.find_all("a")
    home_url = "https://or.wikipedia.org"
    links = [home_url + anchor["href"] for anchor in anchors]
    prev_len = 0
    with SESSION:
        for link in tqdm(links):
            while link:
                response = SESSION.get(link)
                soup = BeautifulSoup(response.text, "html.parser")
                div = soup.find("div", {"class": "mw-allpages-body"})
                if div:
                    anchors = div.find_all("a")
                    all_links = [
                        home_url + anchor["href"]
                        for anchor in anchors
                    ]
                if prev_len == len(set(all_links)):
                    break
                nav_div = soup.find("div", {"class": "mw-allpages-nav"})
                if nav_div and len(nav_div.find_all("a")) == 2:
                    link = home_url + nav_div.find_all("a")[1]["href"]
                prev_len = len(set(all_links))
    return all_links


def write_link_text(all_links_: List[str]) -> None:
    """
    Extracts data from the links and write into the files
    :param all_links_:
    :type all_links_:
    :return:
    :rtype:
    """
    filename = "Not created yet"
    try:
        for counter, url in tqdm(enumerate(all_links_)):
            with SESSION:
                link_response = SESSION.get(url)
            link_soup = BeautifulSoup(link_response.text, "html.parser")
            paras = link_soup.find_all("p")
            article = "\n".join([para.text for para in paras])
            article = process_text(article)
            filename = OUTPUT_PATH + str(counter) + ".txt"
            with open(filename, "w") as output_file:
                output_file.write(article)
    except BaseException as error:
        print(f"unable to write file: {filename} due to: {error}")


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
    return article_text


if __name__ == '__main__':
    write_link_text(fetch_article_links())
