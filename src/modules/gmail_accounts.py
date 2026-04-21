"""Gmail accounts management module.

This module handles:
- Multiple Gmail accounts with OAuth
- Round-robin or weighted rotation
- Per-account daily/hourly limits
- Account isolation and error handling
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class GmailAccount:
    """Represents a Gmail account for sending emails."""
    id: str
    email: str
    credentials_file: str
    weight: int = 1
    daily_limit: int = 30
    hourly_limit: int = 8
    enabled: bool = True
    error_count: int = 0
    success_count: int = 0
    bounce_count: int = 0
    last_used: Optional[datetime] = None
    pause_until: Optional[datetime] = None
    consecutive_errors: int = 0

    # Runtime tracking
    _hourly_sent: int = field(default=0, init=False)
    _hourly_reset: datetime = field(default_factory=datetime.now, init=False)
    _daily_sent: int = field(default=0, init=False)
    _daily_reset: datetime = field(default_factory=datetime.now, init=False)

    def __post_init__(self) -> None:
        """Initialize runtime tracking."""
        self._hourly_reset = datetime.now()
        self._daily_reset = datetime.now().replace(hour=0, minute=0, second=0)

    @property
    def is_paused(self) -> bool:
        """Check if account is paused."""
        if not self.enabled:
            return True
        if self.pause_until and datetime.now() < self.pause_until:
            return True
        return False

    @property
    def error_rate(self) -> float:
        """Calculate error rate as percentage."""
        total = self.success_count + self.error_count
        if total == 0:
            return 0.0
        return (self.error_count / total) * 100

    @property
    def can_send(self) -> bool:
        """Check if account can send emails."""
        if self.is_paused:
            return False

        self._check_rate_limits()

        if self._hourly_sent >= self.hourly_limit:
            return False
        if self._daily_sent >= self.daily_limit:
            return False

        return True

    def _check_rate_limits(self) -> None:
        """Check and reset hourly/daily counters."""
        now = datetime.now()

        # Reset hourly counter every hour
        if (now - self._hourly_reset).total_seconds() >= 3600:
            self._hourly_sent = 0
            self._hourly_reset = now

        # Reset daily counter at midnight
        if now.date() > self._daily_reset.date():
            self._daily_sent = 0
            self._daily_reset = now

    def record_send(self, success: bool) -> None:
        """Record a send attempt."""
        now = datetime.now()
        self._hourly_sent += 1
        self._daily_sent += 1

        if success:
            self.success_count += 1
            self.consecutive_errors = 0
            self.last_used = now
        else:
            self.error_count += 1
            self.consecutive_errors += 1
            logger.warning(f"Account {self.id} has {self.consecutive_errors} consecutive errors")

    def pause(self, minutes: int = 60) -> None:
        """Pause account for specified minutes."""
        self.pause_until = datetime.now() + timedelta(minutes=minutes)
        logger.warning(f"Account {self.id} paused until {self.pause_until}")

    def unpause(self) -> None:
        """Unpause account."""
        self.pause_until = None
        logger.info(f"Account {self.id} unpaused")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "email": self.email,
            "credentials_file": self.credentials_file,
            "weight": self.weight,
            "daily_limit": self.daily_limit,
            "hourly_limit": self.hourly_limit,
            "enabled": self.enabled,
            "error_count": self.error_count,
            "success_count": self.success_count,
            "bounce_count": self.bounce_count,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "pause_until": self.pause_until.isoformat() if self.pause_until else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GmailAccount":
        """Create from dictionary."""
        last_used = None
        if data.get("last_used"):
            try:
                last_used = datetime.fromisoformat(data["last_used"])
            except (ValueError, TypeError):
                pass

        pause_until = None
        if data.get("pause_until"):
            try:
                pause_until = datetime.fromisoformat(data["pause_until"])
            except (ValueError, TypeError):
                pass

        return cls(
            id=data["id"],
            email=data["email"],
            credentials_file=data["credentials_file"],
            weight=data.get("weight", 1),
            daily_limit=data.get("daily_limit", 30),
            hourly_limit=data.get("hourly_limit", 8),
            enabled=data.get("enabled", True),
            error_count=data.get("error_count", 0),
            success_count=data.get("success_count", 0),
            bounce_count=data.get("bounce_count", 0),
            last_used=last_used,
            pause_until=pause_until,
        )


# =============================================================================
# Database Operations
# =============================================================================


class AccountDatabase:
    """Database for storing and tracking Gmail accounts."""

    def __init__(self, db_path: str = "data/accounts.db"):
        self.db_path = db_path

    async def init(self) -> None:
        """Initialize database tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    credentials_file TEXT NOT NULL,
                    weight INTEGER DEFAULT 1,
                    daily_limit INTEGER DEFAULT 30,
                    hourly_limit INTEGER DEFAULT 8,
                    enabled INTEGER DEFAULT 1,
                    error_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    bounce_count INTEGER DEFAULT 0,
                    last_used TEXT,
                    pause_until TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS account_daily_stats (
                    account_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    sent INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    bounced INTEGER DEFAULT 0,
                    PRIMARY KEY (account_id, date)
                )
            """)
            await db.commit()

    async def save_account(self, account: GmailAccount) -> None:
        """Save or update an account."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO accounts (
                    id, email, credentials_file, weight, daily_limit,
                    hourly_limit, enabled, error_count, success_count,
                    bounce_count, last_used, pause_until
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    email = excluded.email,
                    credentials_file = excluded.credentials_file,
                    weight = excluded.weight,
                    daily_limit = excluded.daily_limit,
                    hourly_limit = excluded.hourly_limit,
                    enabled = excluded.enabled,
                    error_count = excluded.error_count,
                    success_count = excluded.success_count,
                    bounce_count = excluded.bounce_count,
                    last_used = excluded.last_used,
                    pause_until = excluded.pause_until
                """,
                (
                    account.id,
                    account.email,
                    account.credentials_file,
                    account.weight,
                    account.daily_limit,
                    account.hourly_limit,
                    1 if account.enabled else 0,
                    account.error_count,
                    account.success_count,
                    account.bounce_count,
                    account.last_used.isoformat() if account.last_used else None,
                    account.pause_until.isoformat() if account.pause_until else None,
                ),
            )
            await db.commit()

    async def get_account(self, account_id: str) -> Optional[GmailAccount]:
        """Get an account by ID."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return GmailAccount.from_dict(dict(row))
        return None

    async def get_all_accounts(self) -> list[GmailAccount]:
        """Get all accounts."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM accounts") as cursor:
                rows = await cursor.fetchall()
                return [GmailAccount.from_dict(dict(row)) for row in rows]

    async def get_enabled_accounts(self) -> list[GmailAccount]:
        """Get all enabled accounts."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM accounts WHERE enabled = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                return [GmailAccount.from_dict(dict(row)) for row in rows]

    async def update_account_stats(
        self, account_id: str, success: bool = True
    ) -> None:
        """Update account statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            if success:
                await db.execute(
                    "UPDATE accounts SET success_count = success_count + 1 WHERE id = ?",
                    (account_id,),
                )
            else:
                await db.execute(
                    "UPDATE accounts SET error_count = error_count + 1 WHERE id = ?",
                    (account_id,),
                )
            await db.commit()

    async def update_account_status(
        self, account_id: str, enabled: bool, pause_until: Optional[str] = None
    ) -> None:
        """Update account status (enable/disable/pause)."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE accounts SET enabled = ?, pause_until = ? WHERE id = ?",
                (1 if enabled else 0, pause_until, account_id),
            )
            await db.commit()

    async def check_daily_limit(self, account_id: str, date: str) -> int:
        """Check how many emails sent today."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """SELECT sent FROM account_daily_stats
                WHERE account_id = ? AND date = ?""",
                (account_id, date),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def record_daily_send(
        self, account_id: str, date: str, success: bool = True
    ) -> None:
        """Record daily send stats."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO account_daily_stats (account_id, date, sent, failed)
                VALUES (?, ?, 1, 0)
                ON CONFLICT(account_id, date) DO UPDATE SET
                    sent = sent + 1
                """,
                (account_id, date),
            )
            if not success:
                await db.execute(
                    """
                    INSERT INTO account_daily_stats (account_id, date, sent, failed)
                    VALUES (?, ?, 0, 1)
                    ON CONFLICT(account_id, date) DO UPDATE SET
                        failed = failed + 1
                    """,
                    (account_id, date),
                )
            await db.commit()

    async def get_stats(self, account_id: str) -> dict[str, Any]:
        """Get account statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
        return {}


# =============================================================================
# Account Manager
# =============================================================================


class AccountManager:
    """Manager for multiple Gmail accounts with rotation."""

    def __init__(
        self,
        credentials_dir: str = "accounts",
        config_path: Optional[str] = None,
    ):
        self.credentials_dir = Path(credentials_dir)
        self.db = AccountDatabase()
        self.accounts: dict[str, GmailAccount] = {}
        self.rotation_strategy = "round_robin"
        self.current_index = 0
        self._load_config(config_path)

    def _load_config(self, config_path: Optional[str] = None) -> None:
        """Load account configuration from config file."""
        if config_path:
            import yaml

            with open(config_path, "r") as f:
                config = yaml.safe_load(f)

            accounts_config = config.get("gmail_accounts", {})
            self.rotation_strategy = accounts_config.get(
                "rotation_strategy", "round_robin"
            )

            for account_cfg in accounts_config.get("accounts", []):
                account = GmailAccount(
                    id=account_cfg["id"],
                    email=account_cfg["email"],
                    credentials_file=account_cfg["credentials_file"],
                    weight=account_cfg.get("weight", 1),
                    daily_limit=account_cfg.get("daily_limit", 30),
                    hourly_limit=account_cfg.get("hourly_limit", 8),
                    enabled=account_cfg.get("enabled", True),
                )
                self.accounts[account.id] = account

    async def initialize(self) -> None:
        """Initialize account database and load accounts."""
        await self.db.init()

        # Load accounts from database
        db_accounts = await self.db.get_all_accounts()
        for account in db_accounts:
            self.accounts[account.id] = account

        # If no accounts in DB, create from config
        if not self.accounts:
            for account in self.accounts.values():
                await self.db.save_account(account)

        logger.info(f"Loaded {len(self.accounts)} Gmail accounts")

    def get_account(self, account_id: str) -> Optional[GmailAccount]:
        """Get account by ID."""
        return self.accounts.get(account_id)

    def get_enabled_accounts(self) -> list[GmailAccount]:
        """Get all enabled accounts that can send."""
        return [
            a
            for a in self.accounts.values()
            if a.enabled and not a.is_paused and a.can_send
        ]

    def get_next_account(self) -> Optional[GmailAccount]:
        """Get the next account to use based on rotation strategy."""
        enabled = self.get_enabled_accounts()
        if not enabled:
            logger.warning("No enabled accounts available")
            return None

        if self.rotation_strategy == "weighted":
            return self._get_weighted_account(enabled)
        else:
            return self._get_round_robin_account(enabled)

    def _get_weighted_account(
        self, enabled: list[GmailAccount]
    ) -> Optional[GmailAccount]:
        """Get account using weighted rotation."""
        import random

        total_weight = sum(a.weight for a in enabled)
        rand_val = random.randint(1, total_weight)
        cumulative = 0

        for account in enabled:
            cumulative += account.weight
            if rand_val <= cumulative:
                return account

        return enabled[0]

    def _get_round_robin_account(
        self, enabled: list[GmailAccount]
    ) -> Optional[GmailAccount]:
        """Get account using round-robin rotation."""
        if not enabled:
            return None

        # Find account with lowest sent count today
        min_sent = min(a._daily_sent for a in enabled)
        candidates = [a for a in enabled if a._daily_sent == min_sent]

        if candidates:
            return candidates[0]

        return enabled[self.current_index % len(enabled)]

    def select_best_account(self) -> Optional[GmailAccount]:
        """Select the best available account."""
        enabled = self.get_enabled_accounts()
        if not enabled:
            return None

        # Priority: least errors, then least usage
        enabled.sort(key=lambda a: (a.error_count, -a.success_count))

        return enabled[0]

    async def record_send(
        self, account_id: str, success: bool = True
    ) -> None:
        """Record a send attempt for an account."""
        account = self.accounts.get(account_id)
        if not account:
            logger.error(f"Account {account_id} not found")
            return

        account.record_send(success)
        await self.db.save_account(account)

        today = datetime.now().strftime("%Y-%m-%d")
        await self.db.record_daily_send(account_id, today, success)

        # Check error thresholds
        await self._check_safety_limits(account)

    async def _check_safety_limits(self, account: GmailAccount) -> None:
        """Check safety limits and pause account if needed."""
        threshold = 5  # 5% error rate

        if account.error_rate > threshold and account.success_count > 10:
            account.pause()
            await self.db.save_account(account)
            logger.warning(
                f"Account {account.id} paused due to high error rate: {account.error_rate:.1f}%"
            )

        if account.consecutive_errors >= 3:
            account.pause(minutes=30)
            await self.db.save_account(account)
            logger.warning(
                f"Account {account.id} paused due to {account.consecutive_errors} consecutive errors"
            )

    async def pause_account(self, account_id: str, minutes: int = 60) -> None:
        """Pause an account."""
        account = self.accounts.get(account_id)
        if account:
            account.pause(minutes)
            await self.db.save_account(account)
            await self.db.update_account_status(
                account_id, False, account.pause_until.isoformat()
            )

    async def enable_account(self, account_id: str) -> None:
        """Enable an account."""
        account = self.accounts.get(account_id)
        if account:
            account.unpause()
            account.enabled = True
            await self.db.save_account(account)
            await self.db.update_account_status(account_id, True, None)

    def get_all_stats(self) -> dict[str, Any]:
        """Get statistics for all accounts."""
        stats = {}
        for account_id, account in self.accounts.items():
            stats[account_id] = {
                "email": account.email,
                "enabled": account.enabled,
                "paused": account.is_paused,
                "success_count": account.success_count,
                "error_count": account.error_count,
                "error_rate": account.error_rate,
                "can_send": account.can_send,
                "daily_sent": account._daily_sent,
                "hourly_sent": account._hourly_sent,
            }
        return stats

    def get_healthy_accounts(self) -> list[GmailAccount]:
        """Get accounts that are healthy and can send."""
        return [
            a
            for a in self.accounts.values()
            if a.enabled
            and not a.is_paused
            and a.can_send
            and a.error_rate < 10
            and a.consecutive_errors < 3
        ]

    async def auto_recover_errors(self) -> None:
        """Attempt to recover paused accounts."""
        for account in self.accounts.values():
            if account.is_paused and account.pause_until:
                # Check if pause time has expired
                if datetime.now() >= account.pause_until:
                    # Reset consecutive errors but keep disabled
                    account.consecutive_errors = 0
                    # Keep enabled=False, require manual re-enable
                    logger.info(
                        f"Account {account.id} pause expired, "
                        f"requires manual re-enable"
                    )