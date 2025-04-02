import warnings
from typing import Dict, Generator, List, Tuple

import pandas as pd
from config import Config
from semanticscholar import SemanticScholar  # type: ignore
from tqdm import tqdm

CITATION_RETRIEVE_LIMIT = 1000

"""
1. raw に格納された ancestry search 対象の論文のメタデータを取得 (paper, paper_id, type の3つのカラムを持つ)
2. 論文のメタデータから reference 情報を取得し，保持
3. reference からダブりを除去
4. ダブりを除去した citation のメタ情報（著者，年代，タイトル，アブストラクト, pid, corpusId, doi) を取得
5. 結果を csv 形式で保存
"""

def load_target_paper_meta_info(config: Config, filename: str ="ancestry_target_paper_list") -> pd.DataFrame:
    target_paper_list_path = config.raw_data_dir / f"{filename}.csv"

    df_target_paper_meta_info = pd.read_csv(target_paper_list_path)

    return df_target_paper_meta_info

def generate_paper_meta_info(df_target_paper_meta_info: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
    for idx in range(len(df_target_paper_meta_info)):
        paper = df_target_paper_meta_info.at[idx, "paper"]
        paper_id_type = df_target_paper_meta_info.at[idx, "paper_id_type"]
        paper_id = df_target_paper_meta_info.at[idx, "paper_id"]

        paper_id_for_search = f"{paper_id_type}:{paper_id}"

        yield paper, paper_id_for_search

def filter_duplicted_items(retrieved_items: List[dict]) -> pd.DataFrame:
    paper_id_set = set()
    for item in retrieved_items:
        paper_id = item["citedPaper"]["paperId"]

        if paper_id is None:
            corpus_id = item["citedPaper"]["corpusId"]
            paper_id = f"CorpusId:{corpus_id}"

        paper_id_set |= set([paper_id])

    df_citing_paper = pd.DataFrame(paper_id_set, columns=["paper_id"])

    return df_citing_paper

def retrieve_paper_meta_info(paper_id: str, semantic_scholar: SemanticScholar) -> Dict[str, str]:
    try:
        paper_meta_info = semantic_scholar.get_paper(
            paper_id,
            fields=["authors", "year", "title", "abstract", "externalIds"]
        )
    except Exception:
        warnings.warn(f"{paper_id} does not found in Semantic Scholar")
        row = {
            "author": "",
            "year": "",
            "title": "",
            "abstract": "",
            "corpus_id": paper_id,
            "doi": ""
        }
        return row

    author_list = paper_meta_info["authors"]
    authors = ", ".join([author["name"] for author in author_list])

    year = paper_meta_info["year"]

    title = paper_meta_info["title"]

    abstract = paper_meta_info["abstract"]

    corpus_id = ""
    doi = ""
    if "CorpusId" in paper_meta_info["externalIds"].keys():
        corpus_id = paper_meta_info["externalIds"]["CorpusId"]
    if "DOI" in paper_meta_info["externalIds"].keys():
        doi = paper_meta_info["externalIds"]["DOI"]

    row = {
        "authors": authors,
        "year": year,
        "title": title,
        "abstract": abstract,
        "corpus_id": corpus_id,
        "doi": doi
    }

    return row

def main() -> None:
    config = Config()
    df_target_paper_meta_info = load_target_paper_meta_info(config)
    semantic_scholar = SemanticScholar()

    pbar = tqdm(total=len(df_target_paper_meta_info))
    retrieved_items = []
    for paper, paper_id_for_search in generate_paper_meta_info(df_target_paper_meta_info):
        pbar.set_description(f"[{paper}] Retrieving citations from Sematinc Scholar...")

        citations = semantic_scholar.get_paper_references(paper_id_for_search, limit=CITATION_RETRIEVE_LIMIT)
        retrieved_items += citations.items

        pbar.update(1)

    df_citing_paper = filter_duplicted_items(retrieved_items)

    data = [] # 1 record ... 1min 程度 → 取りたい情報ごとに get した方が良いかも...？ (10データに 2min 33sec)
    pbar = tqdm(total=len(df_citing_paper), desc="Retrieving meta info of citing papers...")
    for paper_id in df_citing_paper["paper_id"]:
        papeer_meta_info = retrieve_paper_meta_info(paper_id, semantic_scholar)
        data.append(papeer_meta_info)

        pbar.update(1)

    df_forward_search_result = pd.DataFrame(data)
    df_forward_search_result.to_csv(config.processed_data_dir / "ancestry_search_result.csv", index=False)

if __name__ == "__main__":
    main()
