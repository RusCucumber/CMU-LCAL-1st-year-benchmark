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

def filter_ineligible_year_records(df_db_result: pd.DataFrame, eligible_year: int) -> pd.DataFrame:
    mask_eligible = df_db_result["year"] >= eligible_year
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
    df_db_result = filter_ineligible_year_records(df_db_result, config.eligible_pub_year)
    df_db_result = sort_records(df_db_result)

    df_db_result.to_csv(config.processed_data_dir / "db_search_records.csv", index=False)

if __name__ == "__main__":
    main()
