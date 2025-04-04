import re
from typing import Generator, Tuple

import pandas as pd
from bs4 import BeautifulSoup, Tag
from config import Config
from tqdm import tqdm

STUDY_RECORD_DIV_ID = "main-content"
N_PAGES = 2

"""
1. 指定のURLから html を取得
2. beautiful soup で html を解析
3. 著者，タイトル，出版年，アブスト，doi を取得
4. データを保存，次のページへ
"""

def searched_html_generator(config: Config) -> Generator[BeautifulSoup, None, None]:
    load_dir = config.external_data_dir / "intercultural_pragmatics"

    for page in range(1, N_PAGES + 1):
        saerched_html_path = load_dir / f"page_{page}.html"
        with open(saerched_html_path, "r") as f:
            searched_html_list = f.readlines()

        searched_html = "".join(searched_html_list)

        parsed_html = BeautifulSoup(searched_html, "html.parser")

        yield parsed_html

def study_record_div_generator(parsed_html: BeautifulSoup) -> Generator[Tag, None, None]:
    for study_record_div in parsed_html.find_all("div", id=STUDY_RECORD_DIV_ID):
        yield study_record_div # type: ignore

def extract_authors(study_record_div: Tag) -> str:
    authors = study_record_div.find("span", class_="contributors suggested-products__tertiary-author-info me-2")

    if authors is None:
        return ""

    authors_str: str = authors.text # type: ignore
    authors_str = authors_str.replace("\n", "").replace("\"", "").strip()

    while "  " in authors_str:
        authors_str = authors_str.replace("  ", " ")

    return authors_str

def extract_title(study_record_div: Tag) -> str:
    title = study_record_div.find("h3", class_="titleSearchPageResult mb-0") # type: ignore

    title_str: str = title.text # type: ignore
    title_str = title_str.replace("\n", "").replace("\"", "").strip()

    return title_str

def extract_pub_year(study_record_div: Tag) -> str:
    pub_year = study_record_div.find("span", class_="pubDate")

    year_regex_result = re.match(r".*?([0-9]{4}).*?", pub_year.text) # type: ignore
    if year_regex_result:
        return year_regex_result.group(1)

    return ""

def extract_abstract_snippet(study_record_div: Tag) -> str:
    abstract_snippet = study_record_div.find("div", class_="snippets snippetsContent three-line-ellipsis my-2")

    if abstract_snippet is None:
        return ""

    abstract_snippet_str: str = abstract_snippet.text # type: ignore
    abstract_snippet_str = abstract_snippet_str.strip().replace("\n", " ")
    return abstract_snippet_str

def extract_doi(study_record_div: Tag) -> Tuple[str, str]:
    download_link_div = study_record_div.find(
        "div", class_="searchResultActions d-flex flex-wrap mt-2 pt-1"
    ) # type: ignore
    download_link_a = download_link_div.find("a") # type: ignore

    doi = download_link_a["data-doi"]
    doi_link = f"https://doi.org/{doi}"

    return doi, doi_link

def main() -> None:
    config = Config()
    data = []

    pbar = tqdm(desc="Extracting study record meta info...")
    for parsed_html in searched_html_generator(config):
        for study_record_div in study_record_div_generator(parsed_html):
            authors = extract_authors(study_record_div)
            title = extract_title(study_record_div)
            year = extract_pub_year(study_record_div)
            abstract_snippet = extract_abstract_snippet(study_record_div)
            doi, doi_link = extract_doi(study_record_div)

            row = {
                "authors": authors,
                "title": title,
                "year": year,
                "abstract": abstract_snippet,
                "doi": doi,
                "link": doi_link,
                "search_method": "manual_journal_search[intercultural_pragmatics]"
            }
            data.append(row)
            pbar.update(1)

    df_applied_linguistics = pd.DataFrame(data)
    df_applied_linguistics.to_csv(
        config.processed_data_dir / "intercultural_pragmatics_manual_search_result.csv",
        index=False
    )

if __name__ == "__main__":
    main()
