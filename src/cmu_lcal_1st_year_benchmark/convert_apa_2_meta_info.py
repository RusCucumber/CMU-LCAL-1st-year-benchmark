import re
from typing import Dict

import pandas as pd
from config import Config

APA_ANCESTRY_CITING_PAPERS = ["plonsky_zhuang", "mori_mori_mori_et_al"]

"""
1. tsv 形式で ancestry result を取得
2. apa 情報より，以下を取得
    - authors
    - year
    - title
    - doi (if exist)
3. 結果を csv 形式で保存
"""


def load_apa_ancestry_results(config: Config) -> Dict[str, pd.DataFrame]:
    dataset = {}
    for citing_paper in APA_ANCESTRY_CITING_PAPERS:
        apa_ancestry_path = config.external_data_dir / f"{citing_paper}.tsv"
        df_ancestry = pd.read_table(apa_ancestry_path, header=None)

        dataset[citing_paper] = df_ancestry

    return dataset

def extract_authors_from_apa(apa_record: str) -> str:
    authors = apa_record.split("(")[0] # 出版年の括弧の前のみ取得
    authors = authors.strip() # 文字列の先頭・末尾の余計なスペースを除去

    return authors

def extract_year_from_apa(apa_record: str) -> str:
    year_pattern = r"\(([0-9]{4}).*?\)"
    year_regex = re.compile(year_pattern)

    year_regex_result = year_regex.search(apa_record)
    if year_regex_result:
        year = year_regex_result.group(1)
    else:
        year = ""

    return year

def extract_title_from_apa(apa_record: str) -> str:
    apa_segments = apa_record.split(").")
    title_segment = apa_segments[1] # year 以降の箇所を取り出す

    title = title_segment.split(".")[0] # 最初からピリオドで区切ると著者名のイニシャルが引っかかるため，タイトル箇所を対象に

    return title

def extract_doi_from_apa(apa_record: str) -> str:
    if "doi.org" not in apa_record:
        return ""

    doi = apa_record.split("doi.org/")[-1]
    return doi

def preprocess_ancestry_dataset(apa_ancestry_dataset: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    processed_dataset = {}
    for citing_paper, df_ancestry in apa_ancestry_dataset.items():
        df_ancestry_processed = df_ancestry.copy(deep=True)
        df_ancestry_processed.loc[:, "authors"] = df_ancestry[0].apply(extract_authors_from_apa)
        df_ancestry_processed.loc[:, "year"] = df_ancestry[0].apply(extract_year_from_apa)
        df_ancestry_processed.loc[:, "title"] = df_ancestry[0].apply(extract_title_from_apa)
        df_ancestry_processed.loc[:, "doi"] = df_ancestry[0].apply(extract_doi_from_apa)

        processed_dataset[citing_paper] = df_ancestry_processed

    return processed_dataset

def merge_dataset(apa_ancestry_dataset: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataset_list = apa_ancestry_dataset.values()

    df_ancestry_merged = pd.concat(dataset_list)
    df_ancestry_merged = df_ancestry_merged.rename(columns={0: "apa"})

    return df_ancestry_merged

def main() -> None:
    config = Config()
    apa_ancestry_dataset = load_apa_ancestry_results(config)
    apa_ancestry_dataset = preprocess_ancestry_dataset(apa_ancestry_dataset)

    df_ancestry_merged = merge_dataset(apa_ancestry_dataset)

    df_ancestry_merged.to_csv(config.processed_data_dir / "apa_ancestry_search_result.csv", index=False)


if __name__ == "__main__":
    main()
