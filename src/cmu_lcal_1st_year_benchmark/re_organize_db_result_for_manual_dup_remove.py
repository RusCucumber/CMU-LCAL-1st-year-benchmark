from typing import Dict

import pandas as pd
from config import Config

"""
1. doi ベースで重複を除去した DB search の結果を取得
2. データを特定の年で filter (今回は 2010以降のみ)
3. データを authors, year, title の順番で sort
4. 結果を保存
"""


def load_db_result(config: Config, filename: str ="db_search_merged_unique") -> pd.DataFrame:
    db_result_path = config.processed_data_dir / f"{filename}.csv"

    df_db_result = pd.read_csv(db_result_path)

    return df_db_result

def load_additional_ancestry_search_datasets(config: Config) -> Dict[str, pd.DataFrame]:
    ancestry_search_result_path = config.processed_data_dir / "additional_ancestry_search_result.csv"
    apa_ancestry_search_result_path = config.processed_data_dir / "additional_apa_ancestry_search_result.csv"

    df_ancestry_result = pd.read_csv(ancestry_search_result_path)
    df_apa_result = pd.read_csv(apa_ancestry_search_result_path)

    datasets = {
        "ancestry": df_ancestry_result,
        "apa": df_apa_result
    }

    return datasets


def preprocess_semantic_scholar_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    # 1. remove author column
    df_result_processed = df_result.drop(["author", "corpus_id"], axis=1)

    # 2. remove authors, year, title is NA
    df_result_processed = df_result_processed.dropna(
        subset=["authors", "year", "title"], how="all"
    )

    # 3. add empty document_type column
    df_result_processed.loc[:, "document_type"] = ""

    # 4. add link column
    df_result_processed.loc[:, "link"] = "https://doi.org/" + df_result_processed["doi"]

    return df_result_processed

def preprocess_apa_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    # 1. remove apa
    df_result_processed = df_result.drop("apa", axis=1)

    # 2. add empty abstract & document_type
    df_result_processed.loc[:, "abstract"] = ""
    df_result_processed.loc[:, "document_type"] = ""

    # skip the following process because there are no doi in additional apa search result
    # 3. add link column
    # df_result_processed.loc[:, "link"] = "https://doi.org/" + df_result_processed["doi"]

    return df_result_processed

def preprocess(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    datasets_processed = {}
    for dataset_name, df_result in datasets.items():
        if dataset_name == "ancestry":
            df_result_processed = preprocess_semantic_scholar_dataset(df_result)
        elif dataset_name == "apa":
            df_result_processed = preprocess_apa_dataset(df_result)
        else:
            raise KeyError(f"Preprocess for {dataset_name} was not found")

        df_result_processed.loc[:, "search_method"] = dataset_name
        datasets_processed[dataset_name] = df_result_processed

    return datasets_processed

def merge_datasets(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataset_list = datasets.values()

    df_result_merged = pd.concat(dataset_list)

    column_order = ["authors", "year", "title", "abstract", "document_type", "doi", "link", "search_method"]
    df_result_merged = df_result_merged[column_order]

    return df_result_merged

def filter_ineligible_year_records(df_db_result: pd.DataFrame, eligible_year: int) -> pd.DataFrame:
    mask_eligible = df_db_result["year"] < eligible_year
    print(f"{(~mask_eligible).sum()} ineligible records were detected!")

    df_db_result_filtered = df_db_result[mask_eligible].reset_index(drop=True)

    return df_db_result_filtered

def sort_records(df_db_result: pd.DataFrame) -> pd.DataFrame:
    df_db_result_sorted = df_db_result.sort_values(by=["authors", "year", "title"])
    df_db_result_sorted = df_db_result_sorted.reset_index(drop=True)
    return df_db_result_sorted

def main() -> None:
    config = Config()
    df_db_result = load_db_result(config)

    additional_results = load_additional_ancestry_search_datasets(config)
    additional_results = preprocess(additional_results)
    additional_results["db"] = df_db_result

    df_merged = merge_datasets(additional_results)

    df_merged = filter_ineligible_year_records(df_merged, config.eligible_pub_year)
    df_merged = sort_records(df_merged)

    df_merged.to_csv(config.processed_data_dir / "additional_db_search_records.csv", index=False)

if __name__ == "__main__":
    main()
