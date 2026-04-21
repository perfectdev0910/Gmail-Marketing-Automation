"""Main pipeline orchestrator.

This module orchestrates the entire email outreach system:
- Lead loading and validation (from Google Sheets)
- Email generation
- Queue management
- Worker execution
- Safety checks
- Logging to Google Sheets for persistence
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from src.config import ConfigLoader, AppConfig
from src.modules.google_sheets import GoogleSheetsService, Lead
from src.modules.gmail_accounts import AccountManager, GmailAccount
from src.modules.email_template import EmailBuilder, TemplateManager
from src.modules.openai_integration import OpenAIService
from src.modules.queue_system import QueueManager, QueueItem, TimeWindowChecker
from src.modules.gmail_api import GmailClientManager, GmailDatabase
from src.services.logging_service import (
    LoggingDatabase,
    ActivityLogger,
    ReportGenerator,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Email Pipeline
# =============================================================================


class EmailPipeline:
    """Main email outreach pipeline using Google Sheets as the database."""

    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        config_loader = ConfigLoader()
        self.config = config_loader.load(config_path)

        # Initialize components
        self.sheets_service: Optional[GoogleSheetsService] = None
        self.account_manager: Optional[AccountManager] = None
        self.templates: Optional[TemplateManager] = None
        self.openai_service: Optional[OpenAIService] = None
        self.queue_manager: Optional[QueueManager] = None
        self.gmail_db: Optional[GmailDatabase] = None
        self.gmail_client: Optional[GmailClientManager] = None
        self.logs_service: Optional[GoogleSheetsService] = None
        self.activity_logger: Optional[ActivityLogger] = None

    async def initialize(self) -> None:
        """Initialize all components."""
        logger.info("Initializing Email Pipeline...")

        # Initialize Gmail accounts manager
        self.account_manager = AccountManager(
            credentials_dir="accounts",
            config_path="config.yaml",
        )
        await self.account_manager.initialize()

        # Initialize template manager
        self.templates = TemplateManager(
            templates_dir=self.config.email_templates.templates_dir
        )
        self.templates.load_template("outreach")

        # Initialize OpenAI service
        api_key = self.config.openai.api_key
        if api_key:
            self.openai_service = OpenAIService(
                api_key=api_key,
                model=self.config.openai.model,
                temperature=self.config.openai.temperature,
            )

        # Initialize queue (local SQLite for rate limiting)
        self.queue_manager = QueueManager(
            db_path=self.config.queue.sqlite.database,
            min_delay=self.config.sending_limits.min_delay_minutes,
            max_delay=self.config.sending_limits.max_delay_minutes,
            long_pause=self.config.sending_limits.long_pause_minutes,
            emails_before_pause=self.config.sending_limits.emails_before_long_pause,
        )
        await self.queue_manager.init()

        # Initialize Gmail database (local SQLite for sent email tracking)
        self.gmail_db = GmailDatabase(self.config.database.sqlite.database)
        await self.gmail_db.init()

        # Initialize Gmail client manager
        self.gmail_client = GmailClientManager(self.config.database.sqlite.database)
        await self.gmail_client.init()
        for account in self.account_manager.accounts.values():
            self.gmail_client.add_client(
                account_id=account.id,
                credentials_file=account.credentials_file,
                email_address=account.email,
            )

        # Initialize logging (local SQLite for activity logs)
        self.logs_db = LoggingDatabase()
        await self.logs_db.init()
        self.activity_logger = ActivityLogger(self.logs_db)

        logger.info("Pipeline initialized successfully")

    async def load_leads(self) -> int:
        """Load leads from Google Sheets (status tracking in Sheets).

        Returns:
            Number of leads loaded
        """
        config = self.config.sheets

        self.sheets_service = GoogleSheetsService(
            credentials_file=config.credentials_file,
            spreadsheet_id=config.spreadsheet_id,
            sheet_range=config.sheet_range,
        )

        # Get pending leads from Sheets (duplication check done in Sheets)
        new_leads = await self.sheets_service.get_leads_with_dedup(
            db=None,  # No longer needed - dedup in Sheets
            batch_size=config.batch_size,
            status_filter="pending",
        )

        # Update status to 'queued' in Sheets
        queued = 0
        for lead in new_leads:
            if lead.row_index > 0:
                success = await self.sheets_service.update_lead_status(
                    lead.row_index, "queued"
                )
                if success:
                    queued += 1

        await self.activity_logger.log(
            "INFO",
            f"Loaded {queued} leads from Google Sheets (status updated to queued)",
        )

        return queued

    async def process_leads(self) -> int:
        """Process leads from Sheets into queue (via local queue).

        Returns:
            Number of emails queued
        """
        if not self.account_manager or not self.queue_manager:
            raise RuntimeError("Pipeline not initialized")

        # Get queued leads from Sheets (status = 'queued' in Sheets)
        if not self.sheets_service:
            config = self.config.sheets
            self.sheets_service = GoogleSheetsService(
                credentials_file=config.credentials_file,
                spreadsheet_id=config.spreadsheet_id,
                sheet_range=config.sheet_range,
            )

        leads = await self.sheets_service.read_leads(
            batch_size=50,
            status_filter="queued",
        )
        if not leads:
            return 0

        queued = 0
        for lead in leads:
            # Get next available account
            account = self.account_manager.select_best_account()
            if not account:
                logger.warning("No accounts available")
                break

            # Check if can send
            if not account.can_send:
                logger.warning(f"Account {account.id} cannot send")
                continue

            # Generate subject and body using OpenAI (or use template)
            if self.openai_service:
                try:
                    subject, html_body = await self.openai_service.generate_email(
                        first_name=lead.first_name,
                        github_url=lead.github_url,
                    )
                except Exception as e:
                    logger.error(f"Email generation failed: {e}")
                    # Fallback to template
                    subject = "Part-time opportunity for developers"
                    builder = EmailBuilder(self.templates.get_template("outreach"))
                    html_body = builder.build_email(
                        to_email=lead.email,
                        from_email=account.email,
                        subject=subject,
                        first_name=lead.first_name,
                        github_url=lead.github_url,
                    ).as_string()
            else:
                # Use template
                subject = "Part-time opportunity for developers"
                builder = EmailBuilder(self.templates.get_template("outreach"))
                html_body = builder.build_email(
                    to_email=lead.email,
                    from_email=account.email,
                    subject=subject,
                    first_name=lead.first_name,
                    github_url=lead.github_url,
                ).as_string()

            # Queue email
            success = await self.queue_manager.enqueue_email(
                lead_email=lead.email,
                first_name=lead.first_name,
                github_url=lead.github_url,
                from_email=account.email,
                subject=subject,
                body_html=html_body,
                account_id=account.id,
                row_index=lead.row_index,  # Pass row index for status update
            )

            if success:
                # Update lead status in Sheets - mark as processing
                if lead.row_index > 0:
                    await self.sheets_service.update_lead_status(
                        lead.row_index, "processing"
                    )
                queued += 1

                await self.activity_logger.log(
                    "INFO",
                    f"Queued email for {lead.email}",
                    account_id=account.id,
                    lead_email=lead.email,
                )

        logger.info(f"Queued {queued} emails")
        return queued

    async def send_from_queue(self, max_emails: int = 10) -> dict[str, Any]:
        """Send emails from queue.

        Args:
            max_emails: Maximum emails to send

        Returns:
            Stats dictionary
        """
        if not self.queue_manager or not self.gmail_client:
            raise RuntimeError("Pipeline not initialized")

        # Check time window
        time_checker = TimeWindowChecker(
            start_hour=self.config.sending_limits.send_window_start,
            end_hour=self.config.sending_limits.send_window_end,
            skip_weekends=self.config.sending_limits.skip_weekends,
        )

        can_send, reason = time_checker.can_send()
        if not can_send:
            logger.info(f"Cannot send: {reason}")
            return {"sent": 0, "failed": 0, "reason": reason}

        # Get next batch
        items = await self.queue_manager.get_next_batch(max_emails)
        if not items:
            return {"sent": 0, "failed": 0, "reason": "no_emails"}

        sent = 0
        failed = 0

        for item in items:
            # Get delay
            delay = await self.queue_manager.get_send_delay()
            if delay > 0:
                logger.debug(f"Waiting {delay} minutes...")
                await asyncio.sleep(delay * 60)

            # Send
            success, result = await self.gmail_client.send_via_account(
                account_id=item.account_id,
                to_email=item.lead_email,
                subject=item.subject,
                html_body=item.body_html,
            )

            if success:
                await self.queue_manager.mark_sent(item.id)
                await self.account_manager.record_send(item.account_id, True)
                await self.gmail_db.record_sent(
                    lead_email=item.lead_email,
                    from_email=item.from_email,
                    subject=item.subject,
                    body_text=item.body_html[:200],
                    account_id=item.account_id,
                    message_id=result,
                )
                # Update Sheets status to 'sent'
                if item.row_index > 0 and self.sheets_service:
                    await self.sheets_service.update_lead_status(
                        item.row_index, "sent"
                    )
                sent += 1

                await self.activity_logger.log(
                    "INFO",
                    f"Sent email to {item.lead_email}",
                    account_id=item.account_id,
                    lead_email=item.lead_email,
                )
            else:
                await self.queue_manager.mark_failed(item.id, result, retry=True)
                await self.account_manager.record_send(item.account_id, False)
                # Update Sheets status to 'failed'
                if item.row_index > 0 and self.sheets_service:
                    await self.sheets_service.update_lead_status(
                        item.row_index, "failed"
                    )
                failed += 1

                await self.activity_logger.log(
                    "ERROR",
                    f"Failed to send to {item.lead_email}: {result}",
                    account_id=item.account_id,
                    lead_email=item.lead_email,
                )

        return {"sent": sent, "failed": failed}

    async def generate_report(self) -> None:
        """Generate and print daily report."""
        report_gen = ReportGenerator(self.logs_db, self.gmail_db)
        await report_gen.print_report()

    async def run_cycle(self) -> None:
        """Run a complete cycle."""
        logger.info("Starting cycle...")

        # Load new leads
        leads_loaded = await self.load_leads()
        logger.info(f"Loaded {leads_loaded} leads")

        # Process leads to queue
        queued = await self.process_leads()
        logger.info(f"Queued {queued} emails")

        # Send emails
        stats = await self.send_from_queue()
        logger.info(f"Sent: {stats.get('sent')}, Failed: {stats.get('failed')}")

    async def close(self) -> None:
        """Close all connections."""
        if self.openai_service:
            await self.openai_service.close()
        logger.info("Pipeline closed")


# =============================================================================
# Main Entry Point
# =============================================================================


async def main() -> None:
    """Main entry point."""
    from src.services.logging_service import setup_logging

    # Setup logging
    setup_logging(log_level="INFO", log_file="logs/email_system.log")

    # Create and run pipeline
    pipeline = EmailPipeline()
    await pipeline.initialize()

    try:
        await pipeline.run_cycle()
        await pipeline.generate_report()
    finally:
        await pipeline.close()


if __name__ == "__main__":
    asyncio.run(main())