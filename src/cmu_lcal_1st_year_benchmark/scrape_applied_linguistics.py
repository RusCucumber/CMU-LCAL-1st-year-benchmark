from typing import Generator

import requests
from bs4 import BeautifulSoup
from config import APPLIED_LINGUISTICS_SEARCHED_URL

STUDY_RECORD_DIV_CLASS = "sr-list al-article-box al-normal clearfix"

"""
1. 指定のURLから html を取得
2. beautiful soup で html を解析
3. 著者，タイトル，出版年，アブスト，doi を取得
4. データを保存，次のページへ
"""

def get_parsed_html_from_url(url: str) -> BeautifulSoup:
    response = requests.get(url)
    html = response.text
    parsed_html = BeautifulSoup(html, "html.parser")

    return parsed_html

def study_record_div_generator(parsed_html: BeautifulSoup) -> Generator[BeautifulSoup, None, None]:
    for study_record_div in parsed_html.find_all("div", class_=STUDY_RECORD_DIV_CLASS):
        yield study_record_div

def main() -> None:
    config = Config()
    parsed_html = get_parsed_html_from_url(APPLIED_LINGUISTICS_SEARCHED_URL)
    print(parsed_html)
    for study_record_div in study_record_div_generator(parsed_html):
        print(type(study_record_div))
        print(study_record_div)

if __name__ == "__main__":
    main()
