import json
from typing import Any, Dict, Generator, List

import pandas as pd
from config import Config
from serpapi import GoogleScholarSearch  # type: ignore
from tqdm import tqdm

MAXIMUM_TOTAL_ITEMS = 1_000

"""
1. 検索ワードを raw より取得 (json 形式)
2. 検索数を 20 ずつずらしながら，結果を取得
3. 結果を json ファイルとして，raw に保存していく
    - filename は google_search_bkup/{query}_{n-loop}.json とする
    ※ もし途中でエラーになっても，再度検索しないようにする
4. "ornagic_results" の長さが 20 未満になった場合，ループを終了
5. google_search_bkup より json ファイルを読み込み，以下の情報を取得 (読み込みは 4 で実施)
    a. title
    b. publication_info.summary (i.e., 著者名等)
    c. link
"""


def load_serpapi_key(config: Config) -> str:
    serpapi_key_path = config.env_dir / "serpapi_key.json"

    with open(serpapi_key_path, "r") as f:
        serpapi_key_json = json.load(f)

    return serpapi_key_json["privateApiKey"]

def load_query(config: Config, filename: str ="google_scholar_search_keyword") -> str:
    query_path = config.raw_data_dir / f"{filename}.json"

    with open(query_path, "r") as f:
        query_json = json.load(f)

    return query_json["query"]

def google_scholar_retrieve_result_generator(query: str, api_key: str, config: Config) -> Generator[dict, None, None]:
    bkup_dir = config.raw_data_dir / "google_search_bkup"
    if not bkup_dir.exists():
        bkup_dir.mkdir(exist_ok=True, parents=True)

    offset = 0
    n_search = 20
    total_items = MAXIMUM_TOTAL_ITEMS

    while offset < total_items:
        bkup_file_path = bkup_dir / f"{query}_{offset}.json"

        if not bkup_file_path.exists():
            result = GoogleScholarSearch({"q": query, "api_key": api_key, "start": str(offset), "num": str(n_search)})
            result = result.get_dict()

            total_items = result["search_information"]["total_results"]
            offset += n_search

            with open(bkup_file_path, "w") as f:
                json.dump(result, f)

            yield result
            continue

        with open(bkup_file_path, "r") as f:
            result = json.load(f)

        total_items = result["search_information"]["total_results"]
        offset += n_search
        yield result

def extract_meta_info(retrieved_items: Dict[str, Any]) -> List[Dict[str, str]]:
    meta_info_list = []
    for item in retrieved_items["organic_results"]:
        title = item["title"]
        publication_info = item["publication_info"]["summary"]

        link = ""
        if "link" in item.keys():
            link = item["link"]

        meta_info = {
            "title": title,
            "publication_info": publication_info,
            "link": link
        }
        meta_info_list.append(meta_info)

    return meta_info_list


def main() -> None:
    config = Config()

    api_key = load_serpapi_key(config)
    query = load_query(config)

    data = []
    pbar = tqdm(
        google_scholar_retrieve_result_generator(query, api_key, config),
        desc="Retrieving studies from Google Scholar..."
    )
    for result in pbar:
        meta_info_list = extract_meta_info(result)
        data += meta_info_list

    # df として保存し，csv として書き出し
    df_google_scholar_result = pd.DataFrame(data)
    df_google_scholar_result.to_csv(config.processed_data_dir / "google_scholar_result.csv", index=False)

if __name__ == "__main__":
    main()
