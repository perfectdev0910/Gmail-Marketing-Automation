"""Gmail API integration module.

This module handles:
- Gmail API authentication with OAuth
- Message creation (MIME HTML)
- Error handling with retry
- Per-account session handling
"""

import base64
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import aiosqlite
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient import errors
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


# =============================================================================
# Gmail Service
# =============================================================================


class GmailService:
    """Gmail API service for sending emails."""

    def __init__(self, credentials_file: str, email_address: str):
        self.credentials_file = credentials_file
        self.email_address = email_address
        self.service: Optional[Any] = None

    def authenticate(self) -> None:
        """Authenticate with Gmail API."""
        try:
            creds = Credentials.from_authorized_user_info(
                self._load_credentials(),
                ["https://www.googleapis.com/auth/gmail.send"],
            )
            http = AuthorizedHttp(creds)
            self.service = build("gmail", "v1", http=http)
            logger.info(f"Authenticated Gmail for {self.email_address}")
        except Exception as e:
            logger.error(f"Gmail auth error: {e}")
            raise

    def _load_credentials(self) -> dict[str, Any]:
        """Load credentials from file."""
        with open(self.credentials_file, "r") as f:
            return json.load(f)

    def build_raw_message(
        self,
        to_email: str,
        subject: str,
        html_body: str,
    ) -> str:
        """Build raw email message.

        Args:
            to_email: Recipient email
            subject: Email subject
            html_body: HTML body

        Returns:
            Base64url encoded message
        """
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        message = MIMEMultipart("alternative")
        message["To"] = to_email
        message["From"] = self.email_address
        message["Subject"] = subject
        message["MIME-Version"] = "1.0"

        html_part = MIMEText(html_body, "html", "utf-8")
        message.attach(html_part)

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return raw

    def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
    ) -> tuple[bool, str]:
        """Send an email.

        Args:
            to_email: Recipient email
            subject: Email subject
            html_body: HTML body

        Returns:
            Tuple of (success, message_id or error)
        """
        if not self.service:
            self.authenticate()

        try:
            raw_message = self.build_raw_message(to_email, subject, html_body)

            message = (
                self.service.users()
                .messages()
                .send(
                    userId="me",
                    body={"raw": raw_message},
                )
                .execute()
            )

            logger.info(f"Sent email to {to_email}, id: {message.get('id')}")
            return True, message.get("id", "")

        except errors.HttpError as e:
            error_msg = str(e)
            logger.error(f"HTTP error sending to {to_email}: {error_msg}")
            return False, error_msg

        except Exception as e:
            logger.error(f"Error sending to {to_email}: {e}")
            return False, str(e)

    def send_email_with_retry(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        max_retries: int = 3,
    ) -> tuple[bool, str]:
        """Send email with retry logic.

        Args:
            to_email: Recipient email
            subject: Email subject
            html_body: HTML body
            max_retries: Maximum retry attempts

        Returns:
            Tuple of (success, message_id or error)
        """
        import time

        last_error = ""
        for attempt in range(max_retries):
            success, result = self.send_email(to_email, subject, html_body)

            if success:
                return True, result

            last_error = result
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed: {result}"
            )

            if attempt < max_retries - 1:
                wait_time = 2**attempt * 5
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        return False, last_error


# =============================================================================
# Gmail Database
# =============================================================================


class GmailDatabase:
    """Database for tracking sent emails."""

    def __init__(self, db_path: str = "data/emails.db"):
        self.db_path = db_path

    async def init(self) -> None:
        """Initialize database tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sent_emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_email TEXT NOT NULL,
                    from_email TEXT NOT NULL,
                    subject TEXT,
                    body_text TEXT,
                    message_id TEXT,
                    account_id TEXT NOT NULL,
                    status TEXT DEFAULT 'sent',
                    sent_at TEXT NOT NULL,
                    error_message TEXT,
                    UNIQUE(lead_email, from_email)
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_sent_email
                ON sent_emails(lead_email, from_email)
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS followups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_email TEXT NOT NULL,
                    from_email TEXT NOT NULL,
                    original_message_id TEXT,
                    followup_number INTEGER,
                    body_text TEXT,
                    sent_at TEXT,
                    status TEXT DEFAULT 'sent',
                    UNIQUE(lead_email, from_email, followup_number)
                )
            """)
            await db.commit()

    async def record_sent(
        self,
        lead_email: str,
        from_email: str,
        subject: str,
        body_text: str,
        account_id: str,
        message_id: str = "",
        error_message: Optional[str] = None,
    ) -> None:
        """Record a sent email."""
        status = "sent" if not error_message else "failed"

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO sent_emails (
                    lead_email, from_email, subject, body_text,
                    message_id, account_id, status, sent_at, error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead_email,
                    from_email,
                    subject,
                    body_text[:500],
                    message_id,
                    account_id,
                    status,
                    datetime.now().isoformat(),
                    error_message,
                ),
            )
            await db.commit()

    async def is_already_sent(
        self,
        lead_email: str,
        from_email: str,
    ) -> bool:
        """Check if email already sent."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT 1 FROM sent_emails
                WHERE lead_email = ? AND from_email = ?
                """,
                (lead_email.lower(), from_email.lower()),
            ) as cursor:
                return await cursor.fetchone() is not None

    async def get_followup(
        self,
        lead_email: str,
        from_email: str,
        followup_number: int,
    ) -> Optional[dict[str, Any]]:
        """Get a follow-up email."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM followups
                WHERE lead_email = ? AND from_email = ?
                AND followup_number = ?
                """,
                (
                    lead_email.lower(),
                    from_email.lower(),
                    followup_number,
                ),
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def record_followup(
        self,
        lead_email: str,
        from_email: str,
        original_message_id: str,
        followup_number: int,
        body_text: str,
    ) -> None:
        """Record a follow-up email."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO followups (
                    lead_email, from_email, original_message_id,
                    followup_number, body_text, sent_at, status
                )
                VALUES (?, ?, ?, ?, ?, ?, 'sent')
                """,
                (
                    lead_email.lower(),
                    from_email.lower(),
                    original_message_id,
                    followup_number,
                    body_text[:500],
                    datetime.now().isoformat(),
                ),
            )
            await db.commit()

    async def get_daily_stats(self, date: str) -> dict[str, Any]:
        """Get statistics for a specific date."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM sent_emails
                WHERE DATE(sent_at) = ?
                """,
                (date,),
            ) as cursor:
                row = await cursor.fetchone()
                return {
                    "total": row[0] or 0,
                    "sent": row[1] or 0,
                    "failed": row[2] or 0,
                }

    async def get_account_stats(
        self, account_id: str, date: str
    ) -> dict[str, Any]:
        """Get statistics for an account on a specific date."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM sent_emails
                WHERE account_id = ? AND DATE(sent_at) = ?
                """,
                (account_id, date),
            ) as cursor:
                row = await cursor.fetchone()
                return {
                    "total": row[0] or 0,
                    "sent": row[1] or 0,
                    "failed": row[2] or 0,
                }


# =============================================================================
# Gmail Client Manager
# =============================================================================


class GmailClientManager:
    """Manager for multiple Gmail clients."""

    def __init__(self, db_path: str = "data/emails.db"):
        self.db_path = db_path
        self.db = GmailDatabase()
        self.clients: dict[str, GmailService] = {}

    async def init(self) -> None:
        """Initialize database."""
        await self.db.init()

    def add_client(
        self,
        account_id: str,
        credentials_file: str,
        email_address: str,
    ) -> None:
        """Add a Gmail client."""
        self.clients[account_id] = GmailService(
            credentials_file=credentials_file,
            email_address=email_address,
        )

    def get_client(self, account_id: str) -> Optional[GmailService]:
        """Get Gmail client by account ID."""
        return self.clients.get(account_id)

    async def send_via_account(
        self,
        account_id: str,
        to_email: str,
        subject: str,
        html_body: str,
    ) -> tuple[bool, str]:
        """Send email via specific account."""
        client = self.clients.get(account_id)
        if not client:
            return False, f"Account {account_id} not found"

        # Ensure authenticated
        if not client.service:
            client.authenticate()

        return client.send_email_with_retry(to_email, subject, html_body)

    def authenticate_all(self) -> None:
        """Authenticate all clients."""
        for client in self.clients.values():
            if not client.service:
                client.authenticate()