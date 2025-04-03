from typing import Dict

import pandas as pd
from config import Config

"""
1. processed から ancestry/forward search/google_scholar_result の結果を読み取り
2. external から LLBA/ERIC/ProQuest_D&T/PsycINFO の結果を読み取り
3. 1, 2 で読み込んだ結果を１つの DataFrame として保存 (authors,year,title,abstract,doi,document_type,link)
4. doi を基準に重複した行を除去
5. 結果を csv ファイルとして保存
"""


def load_datasets(config: Config) -> Dict[str, pd.DataFrame]:
    ancestry_search_result_path = config.processed_data_dir / "ancestry_search_result.csv"
    forward_search_result_path = config.processed_data_dir / "forward_search_result.csv"
    google_scholar_result_path = config.processed_data_dir / "google_scholar_result.csv"
    apa_ancestry_search_result_path = config.processed_data_dir / "apa_ancestry_search_result.csv"

    eric_result_path = config.external_data_dir / "ERIC_result.xls"
    llba_result_path = config.external_data_dir / "LLBA_result.xls"
    proquest_d_t_result_path = config.external_data_dir / "ProQuest_D&T_result.xls"
    psycinfo_result_path = config.external_data_dir / "PsycINFO_result.csv"

    df_ancestry_result = pd.read_csv(ancestry_search_result_path)
    df_forward_result = pd.read_csv(forward_search_result_path)
    df_google_result = pd.read_csv(google_scholar_result_path)
    df_apa_result = pd.read_csv(apa_ancestry_search_result_path)

    df_eric_result = pd.read_excel(eric_result_path, engine="xlrd")
    df_llba_result = pd.read_excel(llba_result_path, engine="xlrd")
    df_proquest_result = pd.read_excel(proquest_d_t_result_path, engine="xlrd")
    df_psycinfo_result = pd.read_csv(psycinfo_result_path)

    datasets = {
        "ancestry": df_ancestry_result,
        "forward": df_forward_result,
        "google": df_google_result,
        "apa": df_apa_result,
        "eric": df_eric_result,
        "llba": df_llba_result,
        "proquest": df_proquest_result,
        "psycinfo": df_psycinfo_result
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

def preprocess_google_scholar_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    # 1. extract author and year info from publication_info
    authors = df_result["publication_info"].apply(lambda info: info.split("-")[0])
    year = df_result["publication_info"].str.extract(r"([0-9]{4})")

    # 2. remove publication_info
    df_result_processed = df_result.drop("publication_info", axis=1)

    # 3. add authors & year
    df_result_processed.loc[:, "authors"] = authors
    df_result_processed.loc[:, "year"] = year

    # 4. add empty abstract, doi, document_type
    df_result_processed.loc[:, "abstract"] = ""
    df_result_processed.loc[:, "doi"] = ""
    df_result_processed.loc[:, "document_type"] = ""

    return df_result_processed

def preprocess_apa_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    # 1. remove apa
    df_result_processed = df_result.drop("apa", axis=1)

    # 2. add empty abstract & document_type
    df_result_processed.loc[:, "abstract"] = ""
    df_result_processed.loc[:, "document_type"] = ""

    # 3. add link column
    df_result_processed.loc[:, "link"] = "https://doi.org/" + df_result_processed["doi"]

    return df_result_processed

def preprocess_llba_eric_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    target_columns = ["Title", "Abstract", "Authors", "digitalObjectIdentifier", "documentType", "year", "DocumentURL"]
    renamed_columns = ["title", "abstract", "authors", "doi", "document_type", "year", "link"]

    # 1. extract target columns
    df_result_processed = df_result[target_columns]

    # 2. rename columns
    column_mapper = {cur_col:new_col for cur_col, new_col in zip(target_columns, renamed_columns)}
    df_result_processed = df_result_processed.rename(columns=column_mapper)

    return df_result_processed

def preprocess_proquest_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    target_columns = ["Title", "Abstract", "Authors", "documentType", "year", "DocumentURL"]
    renamed_columns = ["title", "abstract", "authors", "document_type", "year", "link"]

    # 1. extract target columns
    df_result_processed = df_result[target_columns]

    # 2. rename columns
    column_mapper = {cur_col:new_col for cur_col, new_col in zip(target_columns, renamed_columns)}
    df_result_processed = df_result_processed.rename(columns=column_mapper)

    # 3. add empty doi column
    df_result_processed.loc[:, "doi"] = ""

    return df_result_processed

def preprocess_psycinfo_dataset(df_result: pd.DataFrame) -> pd.DataFrame:
    target_columns = ["title", "abstract", "publicationDate", "contributors", "docTypes", "doi", "plink"]

    # 1. extract target columns
    df_result_processed = df_result[target_columns]

    # 2. extract and add year
    year = df_result_processed["publicationDate"] // 10000
    df_result_processed.loc[:, "year"] = year

    # 3. drop publicationDate
    df_result_processed = df_result_processed.drop("publicationDate", axis=1)

    # 4. rename columns
    column_mapper = {
        "contributors": "authors",
        "docTypes": "document_type",
        "plink": "link"
    }
    df_result_processed = df_result_processed.rename(columns=column_mapper)

    return df_result_processed

def preprocess(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    datasets_processed = {}
    for dataset_name, df_result in datasets.items():
        if dataset_name in ["ancestry", "forward"]:
            df_result_processed = preprocess_semantic_scholar_dataset(df_result)
        elif dataset_name in ["google"]:
            df_result_processed = preprocess_google_scholar_dataset(df_result)
        elif dataset_name in ["apa"]:
            df_result_processed = preprocess_apa_dataset(df_result)
        elif dataset_name in ["eric", "llba"]:
            df_result_processed = preprocess_llba_eric_dataset(df_result)
        elif dataset_name in ["proquest"]:
            df_result_processed = preprocess_proquest_dataset(df_result)
        elif dataset_name in ["psycinfo"]:
            df_result_processed = preprocess_psycinfo_dataset(df_result)
        else:
            raise KeyError(f"Preprocess for {dataset_name} was not found")

        df_result_processed.loc[:, "search_method"] = dataset_name
        datasets_processed[dataset_name] = df_result_processed

    return datasets_processed

def merge_datasets(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataset_list = datasets.values()

    df_result_merged = pd.concat(dataset_list)

    column_order = ["authors","year","title","abstract","document_type", "doi", "link", "search_method"]
    df_result_merged = df_result_merged[column_order]

    return df_result_merged

def remove_doi_duplicated(df_result_merged: pd.DataFrame) -> pd.DataFrame:
    search_method_priority = {
        "llba": 0,
        "eric": 1,
        "psycinfo": 2,
        "proquest": 3,
        "ancestry": 4,
        "forward": 5,
        "apa": 6,
        "google": 7
    }

    df_result_merged_sorted = df_result_merged.sort_values(
        "search_method",
        key=lambda col: col.map(search_method_priority)
    ).reset_index(drop=True)

    mask_doi_duplicate = df_result_merged_sorted.duplicated(subset="doi", keep="first")
    print(f"{mask_doi_duplicate.sum()} duplicated records were detected!")

    df_result_merged_unique = df_result_merged_sorted[~mask_doi_duplicate].reset_index(drop=True)

    return df_result_merged_unique

def main() -> None:
    config = Config()
    datasets = load_datasets(config)

    datasets_processed = preprocess(datasets)
    df_result_merged = merge_datasets(datasets_processed)
    df_result_merged_unique = remove_doi_duplicated(df_result_merged)

    df_result_merged_unique.to_csv(config.processed_data_dir / "db_search_merged_unique.csv", index=False)

if __name__ == "__main__":
    main()
