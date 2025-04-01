import dataclasses
from pathlib import Path

WORK_DIR = Path(__file__).parents[2]

@dataclasses.dataclass(frozen=True)
class Config:
    raw_data_dir: Path = WORK_DIR / "data/raw"
    processed_data_dir: Path = WORK_DIR / "data/processed"
    external_data_dir: Path = WORK_DIR / "data/external"
    env_dir: Path = WORK_DIR / "environment"
