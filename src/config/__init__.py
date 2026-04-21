"""Configuration loader module.

This module loads and manages configuration from config.yaml and environment variables.
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# =============================================================================
# Pydantic Models
# =============================================================================


class SystemConfig(BaseModel):
    """System configuration."""
    name: str = "Gmail Marketing Automation"
    version: str = "1.0.0"
    environment: str = "production"
    debug: bool = False


class SheetsConfig(BaseModel):
    """Google Sheets configuration."""
    credentials_file: str = "credentials.json"
    spreadsheet_id: str = ""
    sheet_range: str = "Sheet1!A:D"
    batch_size: int = 100
    read_interval_minutes: int = 30


class GmailAccountConfig(BaseModel):
    """Single Gmail account configuration."""
    id: str
    email: str
    credentials_file: str
    weight: int = 1
    daily_limit: int = 30
    hourly_limit: int = 8
    enabled: bool = True
    error_count: int = 0
    success_count: int = 0
    last_used: Optional[str] = None


class GmailAccountsConfig(BaseModel):
    """Gmail accounts configuration."""
    rotation_strategy: str = "round_robin"
    accounts: list[GmailAccountConfig] = []


class SendingLimitsConfig(BaseModel):
    """Sending limits configuration."""
    max_emails_per_account_day: int = 30
    max_emails_per_account_hour: int = 8
    max_emails_per_account_week: int = 150
    min_delay_minutes: int = 3
    max_delay_minutes: int = 7
    long_pause_minutes: int = 15
    emails_before_long_pause: int = 4
    send_window_start: int = 9
    send_window_end: int = 17
    skip_weekends: bool = True
    enable_randomization: bool = True


class SafetyConfig(BaseModel):
    """Safety guard configuration."""
    account_error_threshold: int = 5
    global_error_threshold: int = 10
    bounce_threshold: int = 5
    max_consecutive_errors: int = 3
    enable_deduplication: bool = True
    deduplication_window_days: int = 30


class EmailTemplatesConfig(BaseModel):
    """Email templates configuration."""
    templates_dir: str = "templates"
    main_template: str = "outreach.html"
    variables: list[str] = ["{{firstName}}", "{{github_url}}"]


class OpenAIConfig(BaseModel):
    """OpenAI configuration."""
    api_key: str = ""
    model: str = "gpt-4"
    temperature: float = 0.7
    max_tokens: int = 500
    generate_subject_variations: bool = True
    max_subject_variations: int = 3
    enable_personalization: bool = True
    personalization_max_tokens: int = 100


class RedisConfig(BaseModel):
    """Redis configuration."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str = ""


class SQLiteConfig(BaseModel):
    """SQLite configuration."""
    database: str = "data/queue.db"


class QueueConfig(BaseModel):
    """Queue configuration."""
    type: str = "redis"
    redis: RedisConfig = RedisConfig()
    sqlite: SQLiteConfig = SQLiteConfig()
    pending_queue: str = "email_queue_pending"
    processing_queue: str = "email_queue_processing"
    failed_queue: str = "email_queue_failed"
    max_retries: int = 3
    retry_delay_minutes: int = 5


class DatabaseConfig(BaseModel):
    """Database configuration."""
    type: str = "sqlite"
    sqlite: SQLiteConfig = SQLiteConfig()
    pool_size: int = 10
    max_overflow: int = 20


class FileLoggingConfig(BaseModel):
    """File logging configuration."""
    enabled: bool = True
    path: str = "logs/email_system.log"
    max_size_mb: int = 100
    backup_count: int = 5


class ConsoleLoggingConfig(BaseModel):
    """Console logging configuration."""
    enabled: bool = True


class StructLoggingConfig(BaseModel):
    """Structured logging configuration."""
    enabled: bool = True


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: FileLoggingConfig = FileLoggingConfig()
    console: ConsoleLoggingConfig = ConsoleLoggingConfig()
    structlog: StructLoggingConfig = StructLoggingConfig()


class FollowupConfig(BaseModel):
    """Follow-up configuration."""
    enabled: bool = True
    first_followup_days: int = 3
    first_followup_template: str = "followup_1.html"
    second_followup_days: int = 3
    second_followup_template: str = "followup_2.html"
    max_followups: int = 2
    skip_on_failure: bool = True


class WorkersConfig(BaseModel):
    """Worker configuration."""
    worker_count: int = 2
    check_interval: int = 60
    batch_size: int = 10


class AppConfig(BaseModel):
    """Main application configuration."""
    system: SystemConfig = SystemConfig()
    sheets: SheetsConfig = SheetsConfig()
    gmail_accounts: GmailAccountsConfig = GmailAccountsConfig()
    sending_limits: SendingLimitsConfig = SendingLimitsConfig()
    safety: SafetyConfig = SafetyConfig()
    email_templates: EmailTemplatesConfig = EmailTemplatesConfig()
    openai: OpenAIConfig = OpenAIConfig()
    queue: QueueConfig = QueueConfig()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()
    followup: FollowupConfig = FollowupConfig()
    workers: WorkersConfig = WorkersConfig()


# =============================================================================
# Configuration Loader
# =============================================================================


class ConfigLoader:
    """Configuration loader that reads from config.yaml and environment variables."""

    _instance: Optional["ConfigLoader"] = None
    _config: Optional[AppConfig] = None

    def __new__(cls) -> "ConfigLoader":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._config is None:
            self.load()

    def load(self, config_path: Optional[str] = None) -> AppConfig:
        """Load configuration from file and environment variables."""
        if config_path is None:
            config_path = os.getenv("CONFIG_PATH", "config.yaml")

        # Load from YAML
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, "r") as f:
                config_dict = yaml.safe_load(f)
        else:
            config_dict = {}

        # Override with environment variables
        if "OPENAI_API_KEY" in os.environ:
            if "openai" not in config_dict:
                config_dict["openai"] = {}
            config_dict["openai"]["api_key"] = os.environ["OPENAI_API_KEY"]

        if "SPREADSHEET_ID" in os.environ:
            if "sheets" not in config_dict:
                config_dict["sheets"] = {}
            config_dict["sheets"]["spreadsheet_id"] = os.environ["SPREADSHEET_ID"]

        if "DATABASE_URL" in os.environ:
            if "database" not in config_dict:
                config_dict["database"] = {}
            # Parse database URL if needed

        if "REDIS_HOST" in os.environ:
            if "queue" not in config_dict:
                config_dict["queue"] = {}
            if "redis" not in config_dict["queue"]:
                config_dict["queue"]["redis"] = {}
            config_dict["queue"]["redis"]["host"] = os.environ["REDIS_HOST"]

        if "REDIS_PORT" in os.environ:
            if "queue" not in config_dict:
                config_dict["queue"] = {}
            if "redis" not in config_dict["queue"]:
                config_dict["queue"]["redis"] = {}
            config_dict["queue"]["redis"]["port"] = int(os.environ["REDIS_PORT"])

        # Create config object
        self._config = AppConfig(**config_dict)
        return self._config

    @property
    def config(self) -> AppConfig:
        """Get the loaded configuration."""
        if self._config is None:
            self.load()
        return self._config

    def reload(self) -> AppConfig:
        """Reload configuration."""
        self._config = None
        return self.load()

    def get_gmail_account(self, account_id: str) -> Optional[GmailAccountConfig]:
        """Get a Gmail account by ID."""
        for account in self.config.gmail_accounts.accounts:
            if account.id == account_id:
                return account
        return None

    def get_enabled_accounts(self) -> list[GmailAccountConfig]:
        """Get all enabled Gmail accounts."""
        return [a for a in self.config.gmail_accounts.accounts if a.enabled]

    def get_next_account(self) -> Optional[GmailAccountConfig]:
        """Get the next account to use based on rotation strategy."""
        enabled = self.get_enabled_accounts()
        if not enabled:
            return None

        strategy = self.config.gmail_accounts.rotation_strategy

        if strategy == "weighted":
            # Weighted round-robin
            total_weight = sum(a.weight for a in enabled)
            import random
            rand_val = random.randint(1, total_weight)
            cumulative = 0
            for account in enabled:
                cumulative += account.weight
                if rand_val <= cumulative:
                    return account
            return enabled[0]
        else:
            # Simple round-robin - use account with fewest sends today
            import datetime
            today = datetime.datetime.now().date()
            min_sends = float("inf")
            best_account = None
            for account in enabled:
                # Check today's send count (would need proper tracking)
                if account.success_count < min_sends:
                    min_sends = account.success_count
                    best_account = account
            return best_account or enabled[0]


# Global config instance
config = ConfigLoader().config