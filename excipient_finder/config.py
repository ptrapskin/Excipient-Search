from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_OUTPUT_ROOT = Path(r"C:\Users\traps\OneDrive\Apps\Excipient Finder")


@dataclass
class Config:
    input_root: Path | None          # None when using --fetch
    output_root: Path = field(default_factory=lambda: DEFAULT_OUTPUT_ROOT)
    db_path: Path = field(default=None)
    log_dir: Path = field(default=None)
    csv_dir: Path = field(default=None)
    limit: int | None = None
    debug: bool = False
    write_excluded_debug: bool = False
    resume: bool = False
    broad_recall: bool = False
    known_positives_path: Path | None = None
    write_qa_samples: bool = False
    write_qa_reports: bool = False
    qa_sample_size: int = 25
    keep_zips: bool = False          # retain ZIP files after processing (default: delete)
    fetch: str | None = None         # "rx" | "otc" | "all" — stream-download then process

    def __post_init__(self) -> None:
        if self.db_path is None:
            self.db_path = self.output_root / "excipients.db"
        if self.log_dir is None:
            self.log_dir = self.output_root / "logs"
        if self.csv_dir is None:
            self.csv_dir = self.output_root

    @property
    def qa_dir(self) -> Path:
        return self.output_root / "qa"
