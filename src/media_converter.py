"""
blackroad-media-converter: Media file conversion job tracker and manager.
SQLite persistence at ~/.blackroad/media-converter.db
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional

# ── ANSI colours ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RED    = "\033[91m"
MAGENTA = "\033[95m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "media-converter.db"

SUPPORTED_FORMATS = {
    "video": ["mp4", "avi", "mkv", "mov", "webm", "flv", "wmv"],
    "audio": ["mp3", "wav", "flac", "aac", "ogg", "m4a", "wma"],
    "image": ["jpg", "png", "gif", "bmp", "webp", "tiff", "svg"],
    "document": ["pdf", "docx", "txt", "html", "epub"],
}


# ── Models ────────────────────────────────────────────────────────────────────
class JobStatus(str, Enum):
    PENDING    = "pending"
    RUNNING    = "running"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"


STATUS_COLORS = {
    JobStatus.PENDING:   YELLOW,
    JobStatus.RUNNING:   CYAN,
    JobStatus.COMPLETED: GREEN,
    JobStatus.FAILED:    RED,
    JobStatus.CANCELLED: DIM,
}


@dataclass
class ConversionJob:
    source_path: str
    target_format: str
    id: Optional[int] = None
    status: JobStatus = JobStatus.PENDING
    source_format: str = ""
    output_path: str = ""
    file_size_kb: float = 0.0
    error_message: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None

    def __post_init__(self):
        if not self.source_format:
            self.source_format = Path(self.source_path).suffix.lstrip(".").lower()
        if not self.output_path:
            stem = Path(self.source_path).stem
            self.output_path = str(Path(self.source_path).parent / f"{stem}.{self.target_format}")

    def media_type(self) -> str:
        for mtype, exts in SUPPORTED_FORMATS.items():
            if self.source_format in exts or self.target_format in exts:
                return mtype
        return "unknown"


# ── Core logic ────────────────────────────────────────────────────────────────
class MediaConverter:
    """Job manager for media conversion workflows."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path    TEXT NOT NULL,
                    target_format  TEXT NOT NULL,
                    source_format  TEXT NOT NULL,
                    output_path    TEXT NOT NULL,
                    status         TEXT DEFAULT 'pending',
                    file_size_kb   REAL DEFAULT 0,
                    error_message  TEXT DEFAULT '',
                    created_at     TEXT NOT NULL,
                    updated_at     TEXT NOT NULL,
                    completed_at   TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            conn.commit()

    def add_job(self, source_path: str, target_format: str,
                file_size_kb: float = 0.0) -> ConversionJob:
        job = ConversionJob(source_path=source_path, target_format=target_format.lower(),
                            file_size_kb=file_size_kb)
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO jobs
                   (source_path, target_format, source_format, output_path,
                    status, file_size_kb, error_message, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (job.source_path, job.target_format, job.source_format,
                 job.output_path, job.status.value, job.file_size_kb,
                 job.error_message, job.created_at, job.updated_at),
            )
            job.id = cur.lastrowid
            conn.commit()
        return job

    def update_status(self, job_id: int, status: JobStatus,
                      error_message: str = "") -> bool:
        now = datetime.now().isoformat()
        completed_at = now if status in (JobStatus.COMPLETED, JobStatus.FAILED) else None
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE jobs SET status=?, error_message=?, updated_at=?, completed_at=?
                   WHERE id=?""",
                (status.value, error_message, now, completed_at, job_id),
            )
            conn.commit()
        return cur.rowcount > 0

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 50) -> List[ConversionJob]:
        query = "SELECT * FROM jobs"
        params: list = []
        if status:
            query += " WHERE status = ?"
            params.append(status.value)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_job(r) for r in rows]

    def get_stats(self) -> dict:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as cnt, SUM(file_size_kb) as total_kb FROM jobs GROUP BY status"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        stats = {"total": total, "by_status": {}, "total_size_kb": 0.0}
        for row in rows:
            stats["by_status"][row["status"]] = row["cnt"]
            stats["total_size_kb"] += row["total_kb"] or 0
        return stats

    def export_report(self, output_path: Optional[Path] = None) -> str:
        jobs = self.list_jobs(limit=10000)
        stats = self.get_stats()
        lines = [
            "# Media Converter — Job Report\n",
            f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n",
            "## Summary\n",
            f"- Total jobs: {stats['total']}\n",
            f"- Total data: {stats['total_size_kb'] / 1024:.2f} MB\n",
        ]
        for s, cnt in stats["by_status"].items():
            lines.append(f"- {s.capitalize()}: {cnt}\n")
        lines.append("\n## Jobs\n\n| ID | Source | Format | Status | Created |\n|---|---|---|---|---|\n")
        for j in jobs:
            lines.append(f"| {j.id} | {Path(j.source_path).name} | "
                         f"{j.source_format}→{j.target_format} | {j.status.value} | {j.created_at[:10]} |\n")
        report = "".join(lines)
        if output_path:
            Path(output_path).write_text(report)
        return report

    def cancel_job(self, job_id: int) -> bool:
        return self.update_status(job_id, JobStatus.CANCELLED)

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> ConversionJob:
        j = ConversionJob(
            id=row["id"],
            source_path=row["source_path"],
            target_format=row["target_format"],
            source_format=row["source_format"],
            output_path=row["output_path"],
            status=JobStatus(row["status"]),
            file_size_kb=row["file_size_kb"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )
        return j


# ── Terminal rendering ────────────────────────────────────────────────────────
def _print_header(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 65}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 65}{RESET}\n")


def _print_job(job: ConversionJob) -> None:
    color = STATUS_COLORS.get(job.status, RESET)
    size = f"{job.file_size_kb:.1f} KB" if job.file_size_kb else "—"
    print(f"  {BOLD}#{job.id:>4}{RESET}  {GREEN}{Path(job.source_path).name}{RESET}")
    print(f"         {job.source_format.upper()} → {YELLOW}{job.target_format.upper()}{RESET}"
          f"  [{color}{job.status.value}{RESET}]  {DIM}{size}  {job.created_at[:10]}{RESET}")
    if job.error_message:
        print(f"         {RED}Error: {job.error_message}{RESET}")


def _print_stats(stats: dict) -> None:
    _print_header("🎬  Media Converter — Stats")
    print(f"  {YELLOW}Total jobs  :{RESET}  {stats['total']}")
    print(f"  {YELLOW}Total data  :{RESET}  {stats['total_size_kb'] / 1024:.2f} MB")
    print(f"\n  {BOLD}By status:{RESET}")
    for s, cnt in stats["by_status"].items():
        color = STATUS_COLORS.get(JobStatus(s), RESET)
        bar = "█" * min(cnt, 30)
        print(f"    {s:<12} {color}{bar}{RESET} {cnt}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="media-converter",
        description="BlackRoad Media Converter — job tracker",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List conversion jobs")
    p_list.add_argument("-s", "--status", choices=[s.value for s in JobStatus])
    p_list.add_argument("-n", "--limit", type=int, default=20)

    p_add = sub.add_parser("add", help="Add a conversion job")
    p_add.add_argument("source_path")
    p_add.add_argument("target_format")
    p_add.add_argument("--size-kb", type=float, default=0.0)

    p_status = sub.add_parser("status", help="Update job status")
    p_status.add_argument("job_id", type=int)
    p_status.add_argument("new_status", choices=[s.value for s in JobStatus])
    p_status.add_argument("--error", default="")

    p_cancel = sub.add_parser("cancel", help="Cancel a job")
    p_cancel.add_argument("job_id", type=int)

    sub.add_parser("stats", help="Show statistics")

    p_exp = sub.add_parser("export", help="Export job report")
    p_exp.add_argument("-o", "--output", default="media_jobs_report.md")

    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    converter = MediaConverter()

    if args.command == "list":
        status_filter = JobStatus(args.status) if args.status else None
        jobs = converter.list_jobs(status=status_filter, limit=args.limit)
        _print_header(f"🎬  Conversion Jobs  ({len(jobs)} shown)")
        if not jobs:
            print(f"  {DIM}No jobs found.{RESET}\n")
        for j in jobs:
            _print_job(j)
        print()

    elif args.command == "add":
        job = converter.add_job(args.source_path, args.target_format, args.size_kb)
        print(f"\n{GREEN}✓ Job queued:{RESET} [{job.id}] "
              f"{Path(job.source_path).name} → {job.target_format.upper()}\n")

    elif args.command == "status":
        ok = converter.update_status(args.job_id, JobStatus(args.new_status), args.error)
        if ok:
            print(f"\n{GREEN}✓ Job #{args.job_id} updated to '{args.new_status}'{RESET}\n")
        else:
            print(f"\n{RED}✗ Job #{args.job_id} not found{RESET}\n")

    elif args.command == "cancel":
        ok = converter.cancel_job(args.job_id)
        if ok:
            print(f"\n{GREEN}✓ Job #{args.job_id} cancelled{RESET}\n")
        else:
            print(f"\n{RED}✗ Job #{args.job_id} not found{RESET}\n")

    elif args.command == "stats":
        _print_stats(converter.get_stats())

    elif args.command == "export":
        converter.export_report(Path(args.output))
        print(f"\n{GREEN}✓ Report exported to:{RESET} {args.output}\n")


if __name__ == "__main__":
    main()
