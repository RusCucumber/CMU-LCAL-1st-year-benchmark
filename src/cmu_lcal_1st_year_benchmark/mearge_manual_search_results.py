from pathlib import Path
from typing import Dict

import pandas as pd
from config import Config
from RISparser import readris  # type: ignore

"""
1. dataset を読み込み
2. ris ファイルから必要情報を取り出し (authors, year, title, abstract, doi, link)
3. ProQuest 形式データから必要情報の取り出し
4. EBSCO 形式データから必要情報の取り出し
5. db_search の結果と doi で照会し，重複があれば削除
"""

def ris_2_df(file_path: Path) -> pd.DataFrame:
    with open(file_path, "r") as f:
        records = readris(f)

    df_records = pd.DataFrame(list(records))

    return df_records

def load_manual_search_results(config: Config) -> Dict[str, pd.DataFrame]:
    df_applied_linguistics = pd.read_csv(config.processed_data_dir / "applied_linguistics_manual_search_result.csv")

    df_annual_review_of_al = pd.read_excel(
        config.external_data_dir / "annual_review_of_applied_linguistics.xls",
        engine="xlrd"
    )

    dfeign_language_annals = ris_2_df(config.external_data_dir / "foreign_language_annals.txt")

    df_international_journal_of_al = ris_2_df(
        config.external_data_dir / "international_journal_of_applied_linguistics.txt"
    )

    df_language_learning = ris_2_df(config.external_data_dir / "language_learning.txt")

    df_second_language_research = ris_2_df(config.external_data_dir / "second_language_research.txt")

    df_ssla = pd.read_excel(config.external_data_dir / "studies_of_second_language_acquisition.xls", engine="xlrd")

    df_system = ris_2_df(config.external_data_dir / "system.txt")

    df_journal_of_pragmatics = ris_2_df(config.external_data_dir / "journal_of_pragmatics.ris")

    df_intercultural_pragmatics = pd.read_csv(
        config.processed_data_dir / "intercultural_pragmatics_manual_search_result.csv"
    )

    df_east_asian_pragmatics = pd.read_csv(config.external_data_dir / "east_asian_pragmatics.csv")

    df_japanese_language_and_literature = ris_2_df(config.external_data_dir / "japanese_language_and_literature.txt")

    datasets = {
        "applied_linguistics": df_applied_linguistics,
        "annual_review_of_applied_linguistics": df_annual_review_of_al,
        "foreign_language_annals": dfeign_language_annals,
        "international_journal_of_applied_linguistics": df_international_journal_of_al,
        "language_learning": df_language_learning,
        "second_language_research": df_second_language_research,
        "studies_of_second_language_acquisition": df_ssla,
        "system": df_system,
        "journal_of_pragmatics": df_journal_of_pragmatics,
        "intercultural_pragmatics": df_intercultural_pragmatics,
        "east_asian_pragmatics": df_east_asian_pragmatics,
        "japanese_language_and_literature": df_japanese_language_and_literature
    }

    return datasets

def preprocess_proquest_style_result(df_result: pd.DataFrame) -> pd.DataFrame:
    target_columns = ["Title", "Abstract", "Authors", "digitalObjectIdentifier", "documentType", "year", "DocumentURL"]
    renamed_columns = ["title", "abstract", "authors", "doi", "document_type", "year", "link"]

    # 1. extract target columns
    df_result_processed = df_result[target_columns]

    # 2. rename columns
    column_mapper = {cur_col:new_col for cur_col, new_col in zip(target_columns, renamed_columns)}
    df_result_processed = df_result_processed.rename(columns=column_mapper)

    return df_result_processed

def preprocess_ris_style_result(df_result: pd.DataFrame) -> pd.DataFrame:
    # 1. identify target columns
    target_columns = ["authors", "year", "abstract", "url"]
    df_result_processed = df_result[target_columns]

    # 2. rename url to link
    df_result_processed = df_result_processed.rename(columns={"url": "link"})

    # 3. add title column
    if "title" in df_result.columns:
        df_result_processed.loc[:, "title"] = df_result["title"]
    elif "primary_title" in df_result.columns:
        df_result_processed.loc[:, "title"] = df_result["primary_title"]

    # 4. add doi
    if "doi" in df_result.columns:
        df_result_processed.loc[:, "doi"] = df_result["doi"]
    else:
        df_result_processed.loc[:, "doi"] = ""

    # 5. convert authors from list 2 str
    mask_no_authors = df_result_processed["authors"].isna()
    if mask_no_authors.sum():
        df_result_processed.loc[mask_no_authors, "authors"] = [[""]]
    authors = df_result_processed["authors"].apply(lambda author_list: " ".join(author_list))
    df_result_processed.loc[:, "authors"] = authors

    # 6. remove https://doi.org/ from doi
    df_result_processed.loc[:, "doi"] = df_result_processed["doi"].str.removeprefix("https://doi.org/")

    return df_result_processed

def preprocess_ebsco_style_result(df_result: pd.DataFrame) -> pd.DataFrame:
    target_columns = ["title", "abstract", "publicationDate", "contributors", "docTypes", "doi", "plink"]

    # 1. extract target columns
    df_result_processed = df_result[target_columns].copy(deep=True)

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
    for journal, df_result in datasets.items():
        if journal in ["applied_linguistics", "intercultural_pragmatics"]:
            datasets_processed[journal] = df_result

        elif journal in ["annual_review_of_applied_linguistics", "studies_of_second_language_acquisition"]:
            df_processed = preprocess_proquest_style_result(df_result)
            df_processed.loc[:, "search_method"] = f"manual_search[{journal}]"
            datasets_processed[journal] = df_processed

        elif journal in ["east_asian_pragmatics"]:
            df_processed = preprocess_ebsco_style_result(df_result)
            df_processed.loc[:, "search_method"] = f"manual_search[{journal}]"
            datasets_processed[journal] = df_processed

        else:
            df_processed = preprocess_ris_style_result(df_result)
            df_processed.loc[:, "search_method"] = f"manual_search[{journal}]"
            datasets_processed[journal] = df_processed

    return datasets_processed

def merge_datasets(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataset_list = datasets.values()

    df_result_merged = pd.concat(dataset_list)

    column_order = ["authors","year","title","abstract","document_type", "doi", "link", "search_method"]
    df_result_merged = df_result_merged[column_order]

    return df_result_merged.reset_index(drop=True)

def remove_duplicated_records_by_doi(df_result_merged: pd.DataFrame, config: Config) -> pd.DataFrame:
    df_db_result = pd.read_csv(config.processed_data_dir / "db_search_merged_unique.csv")
    existing_doi_list = df_db_result["doi"].str.lower().tolist()

    duplicated_doi_list = []
    for doi in df_result_merged["doi"]:
        if doi.lower() in existing_doi_list:
            duplicated_doi_list.append(True)
            continue

        duplicated_doi_list.append(False)

    mask_duplicated_doi: pd.Series[bool] = pd.Series(duplicated_doi_list, dtype=bool)
    print(f"{mask_duplicated_doi.sum()} duplicated records were detected!")

    df_result_removed_duplicated_doi = df_result_merged[~mask_duplicated_doi]
    return df_result_removed_duplicated_doi

def main() -> None:
    config = Config()
    datasets = load_manual_search_results(config)
    datasets = preprocess(datasets)

    df_result_merged = merge_datasets(datasets)
    df_result_merged = remove_duplicated_records_by_doi(df_result_merged, config)
    df_result_merged = df_result_merged.sort_values(by=["authors", "year", "title"]).reset_index(drop=True)

    df_result_merged.to_csv(config.processed_data_dir / "manual_search_merged_unique.csv", index=False)

if __name__ == "__main__":
    main()
