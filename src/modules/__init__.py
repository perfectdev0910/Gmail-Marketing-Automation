"""Core modules for the email automation system."""

from .google_sheets import Lead, LeadDatabase, GoogleSheetsService
from .gmail_accounts import GmailAccount, AccountManager
from .email_template import EmailTemplate, EmailBuilder, TemplateManager
from .openai_integration import OpenAIService
from .queue_system import QueueItem, QueueManager, TimeWindowChecker
from .gmail_api import GmailService, GmailClientManager, GmailDatabase