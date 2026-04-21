"""Google Sheets integration module.

This module handles reading leads from Google Sheets with:
- Safe batch reading
- Deduplication
- Email format validation
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import aiosqlite
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from email_validator import validate_email, EmailNotValidError

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class Lead:
    """Represents a lead from Google Sheets."""
    no: int
    user_name: str
    email: str
    github_url: str
    processed: bool = False
    send_attempts: int = 0
    last_attempt: Optional[datetime] = None
    status: str = "pending"  # pending, queued, sent, failed, skipped

    def __post_init__(self) -> None:
        """Validate lead data after initialization."""
        self.user_name = self.user_name.strip() if self.user_name else ""
        self.email = self.email.strip().lower() if self.email else ""
        self.github_url = self.github_url.strip() if self.github_url else ""

    @property
    def first_name(self) -> str:
        """Extract first name from user name."""
        if not self.user_name:
            return ""
        parts = self.user_name.split()
        return parts[0] if parts else ""

    def to_dict(self) -> dict[str, Any]:
        """Convert lead to dictionary."""
        return {
            "no": self.no,
            "user_name": self.user_name,
            "email": self.email,
            "github_url": self.github_url,
            "processed": self.processed,
            "send_attempts": self.send_attempts,
            "last_attempt": self.last_attempt.isoformat() if self.last_attempt else None,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Lead":
        """Create lead from dictionary."""
        last_attempt = None
        if data.get("last_attempt"):
            try:
                last_attempt = datetime.fromisoformat(data["last_attempt"])
            except (ValueError, TypeError):
                pass

        return cls(
            no=int(data.get("no", 0)),
            user_name=data.get("user_name", ""),
            email=data.get("email", ""),
            github_url=data.get("github_url", ""),
            processed=bool(data.get("processed", False)),
            send_attempts=int(data.get("send_attempts", 0)),
            last_attempt=last_attempt,
            status=data.get("status", "pending"),
        )


# =============================================================================
# Validators
# =============================================================================


EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

GITHUB_URL_REGEX = re.compile(
    r"^https?://(?:www\.)?github\.com/[a-zA-Z0-9-]+/[a-zA-Z0-9-._]+/?$"
)


def validate_email_format(email: str) -> tuple[bool, str]:
    """Validate email address format.

    Args:
        email: Email address to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not email:
        return False, "Email is empty"

    # Basic regex validation
    if not EMAIL_REGEX.match(email):
        return False, "Invalid email format"

    # More thorough validation with email-validator
    try:
        validation = validate_email(email, check_deliverability=False)
        email = validation.email  # Normalized
        return True, ""
    except EmailNotValidError as e:
        return False, str(e)


def validate_github_url(url: str) -> tuple[bool, str]:
    """Validate GitHub URL format.

    Args:
        url: GitHub profile URL to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not url:
        return False, "GitHub URL is empty"

    if GITHUB_URL_REGEX.match(url):
        return True, ""

    return False, "Invalid GitHub URL format"


# =============================================================================
# Database Operations
# =============================================================================


class LeadDatabase:
    """Database for storing and tracking leads."""

    def __init__(self, db_path: str = "data/leads.db"):
        self.db_path = db_path

    async def init(self) -> None:
        """Initialize database tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    no INTEGER PRIMARY KEY,
                    user_name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    github_url TEXT,
                    processed INTEGER DEFAULT 0,
                    send_attempts INTEGER DEFAULT 0,
                    last_attempt TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(email)
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)
            """)
            await db.commit()

    async def save_lead(self, lead: Lead) -> bool:
        """Save or update a lead in the database.

        Args:
            lead: Lead to save

        Returns:
            True if lead was saved/updated, False if it already exists
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO leads (no, user_name, email, github_url, processed,
                                    send_attempts, last_attempt, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(email) DO NOTHING
                    """,
                    (
                        lead.no,
                        lead.user_name,
                        lead.email,
                        lead.github_url,
                        1 if lead.processed else 0,
                        lead.send_attempts,
                        lead.last_attempt.isoformat() if lead.last_attempt else None,
                        lead.status,
                    ),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving lead: {e}")
            return False

    async def save_leads(self, leads: list[Lead]) -> int:
        """Save multiple leads.

        Args:
            leads: List of leads to save

        Returns:
            Number of leads saved
        """
        saved = 0
        for lead in leads:
            if await self.save_lead(lead):
                saved += 1
        return saved

    async def get_lead_by_email(self, email: str) -> Optional[Lead]:
        """Get a lead by email address."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM leads WHERE email = ?", (email.lower(),)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Lead.from_dict(dict(row))
        return None

    async def get_unprocessed_leads(
        self, limit: int = 100
    ) -> list[Lead]:
        """Get unprocessed leads that haven't been sent."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM leads
                WHERE processed = 0 AND status IN ('pending', 'failed')
                AND send_attempts < 3
                ORDER BY no
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [Lead.from_dict(dict(row)) for row in rows]

    async def get_queued_leads(self, limit: int = 100) -> list[Lead]:
        """Get leads that are queued for sending."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM leads
                WHERE status = 'queued'
                ORDER BY no
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [Lead.from_dict(dict(row)) for row in rows]

    async def update_lead_status(
        self, email: str, status: str, increment_attempts: bool = True
    ) -> None:
        """Update lead status."""
        async with aiosqlite.connect(self.db_path) as db:
            if increment_attempts:
                await db.execute(
                    """
                    UPDATE leads
                    SET status = ?, send_attempts = send_attempts + 1,
                        last_attempt = ?
                    WHERE email = ?
                    """,
                    (
                        status,
                        datetime.now().isoformat(),
                        email.lower(),
                    ),
                )
            else:
                await db.execute(
                    "UPDATE leads SET status = ? WHERE email = ?",
                    (status, email.lower()),
                )
            await db.commit()

    async def mark_lead_processed(self, email: str) -> None:
        """Mark a lead as processed."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE leads SET processed = 1 WHERE email = ?",
                (email.lower(),),
            )
            await db.commit()

    async def is_duplicate(self, email: str) -> bool:
        """Check if email already exists in database."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT 1 FROM leads WHERE email = ?", (email.lower(),)
            ) as cursor:
                return await cursor.fetchone() is not None

    async def get_stats(self) -> dict[str, Any]:
        """Get lead statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) as queued
                FROM leads
                """
            ) as cursor:
                row = await cursor.fetchone()
                return {
                    "total": row[0] or 0,
                    "sent": row[1] or 0,
                    "failed": row[2] or 0,
                    "pending": row[3] or 0,
                    "queued": row[4] or 0,
                }


# =============================================================================
# Google Sheets Service
# =============================================================================


class GoogleSheetsService:
    """Service for reading leads from Google Sheets."""

    def __init__(
        self,
        credentials_file: str,
        spreadsheet_id: str,
        sheet_range: str = "Sheet1!A:D",
    ):
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.service: Optional[Any] = None

    async def authenticate(self) -> None:
        """Authenticate with Google Sheets API."""
        try:
            creds = Credentials.from_authorized_user_info(
                self._load_credentials(), ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
            http = AuthorizedHttp(creds)
            self.service = build("sheets", "v4", http=http)
            logger.info("Authenticated with Google Sheets API")
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise

    def _load_credentials(self) -> dict[str, Any]:
        """Load credentials from file."""
        import json

        with open(self.credentials_file, "r") as f:
            return json.load(f)

    async def read_leads(self, batch_size: int = 100) -> list[Lead]:
        """Read leads from Google Sheets.

        Args:
            batch_size: Number of leads to read at a time

        Returns:
            List of Lead objects
        """
        if not self.service:
            await self.authenticate()

        try:
            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=self.sheet_range)
                .execute()
            )

            values = result.get("values", [])
            if not values:
                logger.warning("No data found in spreadsheet")
                return []

            # Skip header row
            data_rows = values[1:]

            leads = []
            for i, row in enumerate(data_rows[:batch_size], start=1):
                # Support 3-column format: User Name, email, github_url
                # Or 4-column format: No, User Name, email, github_url
                if len(row) < 2:
                    continue

                # Handle column positions based on count
                if len(row) >= 4:
                    # 4 columns: No, User Name, email, github_url
                    user_name = row[1].strip()
                    email = row[2].strip().lower()
                    github_url = row[3].strip() if len(row) > 3 else ""
                else:
                    # 3 columns: User Name, email, github_url
                    user_name = row[0].strip()
                    email = row[1].strip().lower()
                    github_url = row[2].strip() if len(row) > 2 else ""

                # Validate email
                is_valid, error = validate_email_format(email)
                if not is_valid:
                    logger.warning(f"Invalid email for row {i}: {error}")
                    continue

                # Validate GitHub URL if provided
                if github_url:
                    is_valid, error = validate_github_url(github_url)
                    if not is_valid:
                        logger.warning(f"Invalid GitHub URL for row {i}: {error}")
                        github_url = ""  # Allow without URL

                lead = Lead(
                    no=i,
                    user_name=user_name,
                    email=email,
                    github_url=github_url,
                )
                leads.append(lead)

            logger.info(f"Read {len(leads)} leads from Google Sheets")
            return leads

        except Exception as e:
            logger.error(f"Error reading leads: {e}")
            raise

    async def get_leads_with_dedup(
        self, db: LeadDatabase, batch_size: int = 100
    ) -> list[Lead]:
        """Read leads and filter duplicates.

        Args:
            db: LeadDatabase instance for deduplication
            batch_size: Maximum leads to return

        Returns:
            List of unique, validated leads
        """
        all_leads = await self.read_leads(batch_size)

        unique_leads = []
        for lead in all_leads:
            if await db.is_duplicate(lead.email):
                logger.debug(f"Skipping duplicate: {lead.email}")
                continue
            unique_leads.append(lead)

        logger.info(
            f"Found {len(unique_leads)} new leads after deduplication"
        )
        return unique_leads