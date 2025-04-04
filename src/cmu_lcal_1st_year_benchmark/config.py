import dataclasses
from pathlib import Path

WORK_DIR = Path(__file__).parents[2]

APPLIED_LINGUISTICS_SEARCHED_URL = "https://academic.oup.com/applij/search-results?allJournals=1&f_TocHeadingTitleList=ArticlesANDArticle&fl_SiteID=5135&rg_ArticleDate=01%2f01%2f2010+TO+12%2f31%2f2999&dateFilterType=range&rg_AllPublicationDates=01%2f01%2f2010+TO+12%2f31%2f2999&qb=%7B%22_text_1%22%3a%22pragmatics%22%2c%22qOp2%22%3a%22AND%22%2c%22_text_2%22%3a%22%E2%80%9CL2+Japanese%E2%80%9D+OR+%E2%80%9CJapanese+as+second+language%E2%80%9D+OR+%E2%80%9CJapanese+as+foreign+language%E2%80%9D%22%2c%22qOp3%22%3a%22AND%22%2c%22_text_3%22%3a%22instruction*+OR+teach*%22%7D&cqb=%5b%7B%22terms%22%3a%5b%7B%22filter%22%3a%22_text_%22%2c%22input%22%3a%22pragmatics%22%7D%2c%7B%22condition%22%3a%22AND%22%2c%22filter%22%3a%22_text_%22%2c%22input%22%3a%22%E2%80%9CL2+Japanese%E2%80%9D+OR+%E2%80%9CJapanese+as+second+language%E2%80%9D+OR+%E2%80%9CJapanese+as+foreign+language%E2%80%9D%22%7D%2c%7B%22condition%22%3a%22AND%22%2c%22filter%22%3a%22_text_%22%2c%22input%22%3a%22instruction*+OR+teach*%22%7D%5d%7D%5d"

@dataclasses.dataclass(frozen=True)
class Config:
    raw_data_dir: Path = WORK_DIR / "data/raw"
    processed_data_dir: Path = WORK_DIR / "data/processed"
    external_data_dir: Path = WORK_DIR / "data/external"
    env_dir: Path = WORK_DIR / "environment"

    eligible_pub_year = 2010
