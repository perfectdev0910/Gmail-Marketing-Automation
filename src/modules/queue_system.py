"""Queue system module.

This module implements a queue-based architecture for email delivery:
- Lead queue (pending, processing, failed)
- Worker system for sending
- Rate limiting
- Timing controls
"""

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)


# =============================================================================
# Queue Item
# =============================================================================


@dataclass
class QueueItem:
    """Represents an email in the queue."""
    id: str
    lead_email: str
    first_name: str
    github_url: str
    from_email: str
    subject: str
    body_html: str
    account_id: str
    priority: int = 0
    status: str = "pending"  # pending, processing, sent, failed, retry
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)
    scheduled_at: datetime = field(default_factory=datetime.now)
    sent_at: Optional[datetime] = None
    error_message: Optional[str] = None
    row_index: int = 0  # Row index in Sheets for status update

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "lead_email": self.lead_email,
            "first_name": self.first_name,
            "github_url": self.github_url,
            "from_email": self.from_email,
            "subject": self.subject,
            "body_html": self.body_html,
            "account_id": self.account_id,
            "priority": self.priority,
            "status": self.status,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat(),
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
            "error_message": self.error_message,
            "row_index": self.row_index,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueItem":
        """Create from dictionary."""
        created_at = datetime.now()
        if data.get("created_at"):
            try:
                created_at = datetime.fromisoformat(data["created_at"])
            except (ValueError, TypeError):
                pass

        scheduled_at = datetime.now()
        if data.get("scheduled_at"):
            try:
                scheduled_at = datetime.fromisoformat(data["scheduled_at"])
            except (ValueError, TypeError):
                pass
        if data.get("scheduled_at"):
            try:
                scheduled_at = datetime.fromisoformat(data["scheduled_at"])
            except (ValueError, TypeError):
                pass

        sent_at = None
        if data.get("sent_at"):
            try:
                sent_at = datetime.fromisoformat(data["sent_at"])
            except (ValueError, TypeError):
                pass

        return cls(
            id=data["id"],
            lead_email=data["lead_email"],
            first_name=data["first_name"],
            github_url=data["github_url"],
            from_email=data["from_email"],
            subject=data["subject"],
            body_html=data["body_html"],
            account_id=data["account_id"],
            priority=data.get("priority", 0),
            status=data.get("status", "pending"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            created_at=created_at,
            scheduled_at=scheduled_at,
            sent_at=sent_at,
            error_message=data.get("error_message"),
        )


# =============================================================================
# Queue Database
# =============================================================================


class QueueDatabase:
    """SQLite-based queue storage."""

    def __init__(self, db_path: str = "data/queue.db"):
        self.db_path = db_path

    async def init(self) -> None:
        """Initialize queue tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS email_queue (
                    id TEXT PRIMARY KEY,
                    lead_email TEXT NOT NULL,
                    first_name TEXT,
                    github_url TEXT,
                    from_email TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body_html TEXT,
                    account_id TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    created_at TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    sent_at TEXT,
                    error_message TEXT,
                    created_at_idx INTEGER
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_status
                ON email_queue(status, scheduled_at)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_email
                ON email_queue(lead_email)
            """)
            await db.commit()

    async def enqueue(self, item: QueueItem) -> bool:
        """Add item to queue."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO email_queue (
                        id, lead_email, first_name, github_url, from_email,
                        subject, body_html, account_id, priority, status,
                        retry_count, max_retries, created_at, scheduled_at,
                        sent_at, error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.id,
                        item.lead_email,
                        item.first_name,
                        item.github_url,
                        item.from_email,
                        item.subject,
                        item.body_html,
                        item.account_id,
                        item.priority,
                        item.status,
                        item.retry_count,
                        item.max_retries,
                        item.created_at.isoformat(),
                        item.scheduled_at.isoformat(),
                        item.sent_at.isoformat() if item.sent_at else None,
                        item.error_message,
                    ),
                )
                await db.commit()
                logger.debug(f"Enqueued: {item.id}")
                return True
        except Exception as e:
            logger.error(f"Error enqueuing: {e}")
            return False

    async def dequeue(self, limit: int = 10) -> list[QueueItem]:
        """Get items ready for processing."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            now = datetime.now().isoformat()
            async with db.execute(
                """
                SELECT * FROM email_queue
                WHERE status = 'pending' AND scheduled_at <= ?
                ORDER BY priority DESC, scheduled_at ASC
                LIMIT ?
                """,
                (now, limit),
            ) as cursor:
                rows = await cursor.fetchall()
                return [QueueItem.from_dict(dict(row)) for row in rows]

    async def mark_processing(self, item_id: str) -> bool:
        """Mark item as processing."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE email_queue SET status = 'processing' WHERE id = ?",
                (item_id,),
            )
            await db.commit()
            return True

    async def mark_sent(self, item_id: str) -> bool:
        """Mark item as sent."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE email_queue
                SET status = 'sent', sent_at = ?
                WHERE id = ?
                """,
                (datetime.now().isoformat(), item_id),
            )
            await db.commit()
            return True

    async def mark_failed(
        self, item_id: str, error_message: str, retry: bool = False
    ) -> bool:
        """Mark item as failed or retry."""
        async with aiosqlite.connect(self.db_path) as db:
            if retry:
                await db.execute(
                    """
                    UPDATE email_queue
                    SET status = 'retry',
                        error_message = ?,
                        retry_count = retry_count + 1,
                        scheduled_at = ?
                    WHERE id = ?
                    """,
                    (
                        error_message,
                        (datetime.now() + timedelta(minutes=5)).isoformat(),
                        item_id,
                    ),
                )
            else:
                await db.execute(
                    "UPDATE email_queue SET status = 'failed', error_message = ? WHERE id = ?",
                    (error_message, item_id),
                )
            await db.commit()
            return True

    async def remove(self, item_id: str) -> bool:
        """Remove item from queue."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM email_queue WHERE id = ?", (item_id,)
            )
            await db.commit()
            return True

    async def get_status_counts(self) -> dict[str, int]:
        """Get count by status."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT status, COUNT(*) as count
                FROM email_queue
                GROUP BY status
                """
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    async def get_pending_count(self) -> int:
        """Get count of pending items."""
        counts = await self.get_status_counts()
        return counts.get("pending", 0)


# =============================================================================
# Queue Manager
# =============================================================================


class QueueManager:
    """Manager for the email queue."""

    def __init__(
        self,
        db_path: str = "data/queue.db",
        min_delay: int = 3,
        max_delay: int = 7,
        long_pause: int = 15,
        emails_before_pause: int = 4,
    ):
        self.db = QueueDatabase(db_path)
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.long_pause = long_pause
        self.emails_before_pause = emails_before_pause
        self._sent_count = 0
        self._long_pause_next = False

    async def init(self) -> None:
        """Initialize queue database."""
        await self.db.init()

    async def enqueue_email(
        self,
        lead_email: str,
        first_name: str,
        github_url: str,
        from_email: str,
        subject: str,
        body_html: str,
        account_id: str,
        scheduled_at: Optional[datetime] = None,
        row_index: int = 0,
    ) -> bool:
        """Add email to queue."""
        import uuid

        item = QueueItem(
            id=str(uuid.uuid4()),
            lead_email=lead_email,
            first_name=first_name,
            github_url=github_url,
            from_email=from_email,
            subject=subject,
            body_html=body_html,
            account_id=account_id,
            scheduled_at=scheduled_at or datetime.now(),
            row_index=row_index,
        )

        return await self.db.enqueue(item)

    async def get_next_batch(self, limit: int = 10) -> list[QueueItem]:
        """Get next batch of emails."""
        return await self.db.dequeue(limit)

    async def mark_sent(self, item_id: str) -> None:
        """Mark as sent and apply rate limiting."""
        await self.db.mark_sent(item_id)
        self._sent_count += 1

        # Determine if next should be long pause
        if self._sent_count % self.emails_before_pause == 0:
            self._long_pause_next = True

    async def mark_failed(
        self, item_id: str, error_message: str, retry: bool = True
    ) -> None:
        """Mark as failed with optional retry."""
        await self.db.mark_failed(item_id, error_message, retry)

    async def get_send_delay(self) -> int:
        """Calculate delay before next send."""
        if self._long_pause_next:
            self._long_pause_next = False
            return self.long_pause

        return random.randint(self.min_delay, self.max_delay)

    async def get_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        counts = await self.db.get_status_counts()
        return {
            "pending": counts.get("pending", 0),
            "processing": counts.get("processing", 0),
            "sent": counts.get("sent", 0),
            "failed": counts.get("failed", 0),
            "retry": counts.get("retry", 0),
        }


# =============================================================================
# Time Window Checker
# =============================================================================


class TimeWindowChecker:
    """Check if current time is within allowed sending window."""

    def __init__(
        self,
        start_hour: int = 9,
        end_hour: int = 17,
        skip_weekends: bool = True,
    ):
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.skip_weekends = skip_weekends

    def can_send(self) -> tuple[bool, str]:
        """Check if emails can be sent now.

        Returns:
            Tuple of (can_send, reason)
        """
        now = datetime.now()

        # Check weekend
        if self.skip_weekends and now.weekday() >= 5:
            return False, "Weekend - skipping"

        # Check hour window
        hour = now.hour
        if hour < self.start_hour:
            return False, f"Before send window (opens at {self.start_hour}:00)"
        if hour >= self.end_hour:
            return False, f"After send window (closes at {self.end_hour}:00)"

        return True, "OK"

    def get_next_valid_time(self) -> datetime:
        """Get next valid time to send."""
        now = datetime.now()

        # If weekend, go to Monday
        if self.skip_weekends and now.weekday() >= 5:
            days_until_monday = 7 - now.weekday()
            return now.replace(hour=self.start_hour, minute=0, second=0) + timedelta(
                days=days_until_monday
            )

        # If before start
        if now.hour < self.start_hour:
            return now.replace(hour=self.start_hour, minute=0, second=0)

        # If after end
        if now.hour >= self.end_hour:
            return now.replace(hour=self.start_hour, minute=0, second=0) + timedelta(days=1)

        return now

    def sleep_until_valid(self) -> None:
        """Sleep if outside valid window."""
        can_send, reason = self.can_send()
        if not can_send:
            logger.info(f"Outside send window: {reason}")
            next_time = self.get_next_valid_time()
            wait_seconds = (next_time - datetime.now()).total_seconds()
            if wait_seconds > 0:
                logger.info(f"Sleeping for {wait_seconds/60:.1f} minutes")
                asyncio.sleep(int(wait_seconds))