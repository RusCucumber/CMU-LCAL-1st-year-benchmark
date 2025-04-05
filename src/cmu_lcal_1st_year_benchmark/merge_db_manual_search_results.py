
from typing import Dict

import pandas as pd
from config import Config


def load_dataset(config: Config) -> Dict[str, pd.DataFrame]:
    df_db_search_result = pd.read_excel(
        config.external_data_dir / "db_search_records_manual_dup_remove.xlsx",
        engine="openpyxl"
    )

    df_manual_search_result = pd.read_csv(config.processed_data_dir / "manual_search_merged_unique.csv")

    dataset = {
        "db_search": df_db_search_result,
        "manual_search": df_manual_search_result
    }
    return dataset

def preprocess_db_search_result(df_result: pd.DataFrame) -> pd.DataFrame:
    mask_duplicated = (df_result["is_duplicated"] == 1)

    print(f"{mask_duplicated.sum()} duplicated records were manually detected!")
    df_result_processed = df_result[~mask_duplicated]
    df_result_processed = df_result_processed.reset_index(drop=True)

    df_result_processed = df_result_processed.drop("is_duplicated", axis=1)

    return df_result_processed

def merge_results(df_db_search_result: pd.DataFrame, df_manual_search_result: pd.DataFrame) -> pd.DataFrame:
    df_merged_result = pd.concat([df_db_search_result, df_manual_search_result])
    df_merged_result.loc[:, "is_eligible"] = ""
    df_merged_result = df_merged_result.sort_values(by=["authors", "year", "title"])
    df_merged_result = df_merged_result.reset_index(drop=True)

    new_col_order = [
        "authors", "year", "title", "abstract", "is_eligible", "document_type", "doi", "link", "search_method"
    ]
    df_merged_result = df_merged_result[new_col_order]

    return df_merged_result

def main() -> None:
    config = Config()
    dataset = load_dataset(config)

    df_db_search_result = dataset["db_search"]
    df_manual_search_result = dataset["manual_search"]

    df_db_search_result = preprocess_db_search_result(df_db_search_result)

    df_merged_result = merge_results(df_db_search_result, df_manual_search_result)

    df_merged_result.to_csv(config.processed_data_dir / "db_manual_search_records.csv", encoding="utf-16", index=False)

if __name__ == "__main__":
    main()
