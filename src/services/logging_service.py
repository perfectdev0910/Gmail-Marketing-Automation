"""Logging and reporting module.

This module handles:
- Structured logging
- Database logging
- Daily reporting
"""

import json
import logging
import logging.handlers
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)


# =============================================================================
# Logging Database
# =============================================================================


class LoggingDatabase:
    """Database for logging and tracking."""

    def __init__(self, db_path: str = "data/logs.db"):
        self.db_path = db_path

    async def init(self) -> None:
        """Initialize logging tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    account_id TEXT,
                    lead_email TEXT,
                    extra_json TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_level
                ON activity_log(level, created_at)
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date TEXT PRIMARY KEY,
                    total_sent INTEGER DEFAULT 0,
                    total_failed INTEGER DEFAULT 0,
                    accounts_used TEXT,
                    notes TEXT
                )
            """)
            await db.commit()

    async def log(
        self,
        level: str,
        message: str,
        account_id: Optional[str] = None,
        lead_email: Optional[str] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        """Log an activity."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO activity_log (
                    level, message, account_id, lead_email, extra_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    level,
                    message,
                    account_id,
                    lead_email,
                    json.dumps(extra) if extra else None,
                    datetime.now().isoformat(),
                ),
            )
            await db.commit()

    async def get_logs(
        self,
        limit: int = 100,
        level: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        """Get recent logs."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            query = "SELECT * FROM activity_log"
            params = []

            conditions = []
            if level:
                conditions.append("level = ?")
                params.append(level)
            if since:
                conditions.append("created_at >= ?")
                params.append(since.isoformat())

            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def record_daily_stat(
        self,
        date: str,
        total_sent: int,
        total_failed: int,
        accounts_used: list[str],
    ) -> None:
        """Record daily statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO daily_stats (date, total_sent, total_failed, accounts_used)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    total_sent = excluded.total_sent,
                    total_failed = excluded.total_failed,
                    accounts_used = excluded.accounts_used
                """,
                (
                    date,
                    total_sent,
                    total_failed,
                    json.dumps(accounts_used),
                ),
            )
            await db.commit()

    async def get_stats_for_date(self, date: str) -> Optional[dict[str, Any]]:
        """Get stats for a specific date."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM daily_stats WHERE date = ?", (date,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None


# =============================================================================
# Structured Logger
# =============================================================================


class ActivityLogger:
    """Structured activity logger."""

    def __init__(
        self,
        db: Optional[LoggingDatabase] = None,
        log_to_console: bool = True,
        log_to_file: bool = True,
    ):
        self.db = db
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file

    async def log(
        self,
        level: str,
        message: str,
        account_id: Optional[str] = None,
        lead_email: Optional[str] = None,
        **extra,
    ) -> None:
        """Log an activity."""
        if self.db:
            await self.db.log(level, message, account_id, lead_email, extra)

        if self.log_to_console:
            log_method = getattr(logger, level.lower(), logger.info)
            parts = [message]
            if account_id:
                parts.append(f"account={account_id}")
            if lead_email:
                parts.append(f"lead={lead_email}")
            log_method(" | ".join(parts))


# =============================================================================
# Report Generator
# =============================================================================


class ReportGenerator:
    """Generate daily reports."""

    def __init__(
        self,
        logs_db: LoggingDatabase,
        gmail_db: Any,
    ):
        self.logs_db = logs_db
        self.gmail_db = gmail_db

    async def generate_daily_report(self, date: Optional[str] = None) -> dict[str, Any]:
        """Generate daily report.

        Args:
            date: Date in YYYY-MM-DD format (defaults to today)

        Returns:
            Report dictionary
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Get email stats
        email_stats = await self.gmail_db.get_daily_stats(date)

        # Get logs count
        logs = await self.logs_db.get_logs(limit=1000, since=datetime.fromisoformat(date))

        error_logs = [l for l in logs if l["level"] in ["ERROR", "CRITICAL"]]
        warning_logs = [l for l in logs if l["level"] == "WARNING"]

        report = {
            "date": date,
            "email_stats": {
                "total": email_stats.get("total", 0),
                "sent": email_stats.get("sent", 0),
                "failed": email_stats.get("failed", 0),
                "success_rate": (
                    email_stats["sent"] / email_stats["total"] * 100
                    if email_stats.get("total", 0) > 0
                    else 0
                ),
            },
            "logs": {
                "total": len(logs),
                "errors": len(error_logs),
                "warnings": len(warning_logs),
            },
            "errors": [l["message"] for l in error_logs[-5:]],
        }

        return report

    async def print_report(self, date: Optional[str] = None) -> None:
        """Print daily report to console."""
        report = await self.generate_daily_report(date)

        print("\n" + "=" * 50)
        print(f"DAILY REPORT - {report['date']}")
        print("=" * 50)

        print(f"\n📧 Email Stats:")
        print(f"   Total: {report['email_stats']['total']}")
        print(f"   Sent: {report['email_stats']['sent']}")
        print(f"   Failed: {report['email_stats']['failed']}")
        print(
            f"   Success Rate: {report['email_stats']['success_rate']:.1f}%"
        )

        print(f"\n📝 Log Summary:")
        print(f"   Total Logs: {report['logs']['total']}")
        print(f"   Errors: {report['logs']['errors']}")
        print(f"   Warnings: {report['logs']['warnings']}")

        if report["errors"]:
            print(f"\n⚠️ Recent Errors:")
            for error in report["errors"]:
                print(f"   - {error}")

        print("=" * 50 + "\n")

    async def save_report(self, date: Optional[str] = None) -> None:
        """Save daily report to database."""
        report = await self.generate_daily_report(date)

        await self.logs_db.record_daily_stat(
            date=report["date"],
            total_sent=report["email_stats"]["sent"],
            total_failed=report["email_stats"]["failed"],
            accounts_used=[],  # Would need to track this separately
        )


# =============================================================================
# Setup Logging
# =============================================================================


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = "logs/email_system.log",
    structlog: bool = False,
) -> logging.Logger:
    """Setup logging configuration."""
    logger = logging.getLogger()

    # Set level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Clear handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=5,
        )
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


async def init_logging(log_file: str = "data/logs.db") -> tuple[LoggingDatabase, ActivityLogger]:
    """Initialize logging system."""
    db = LoggingDatabase(log_file)
    await db.init()

    logger_db = LoggingDatabase(log_file)
    await logger_db.init()

    activity_logger = ActivityLogger(logger_db)

    return logger_db, activity_logger