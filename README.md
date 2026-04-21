# Gmail Marketing Automation System

A production-ready Python email outreach automation system with multi-account Gmail support, queue-based architecture, and deliverability controls.

## Features

- **Google Sheets Integration** - Read leads from Google Sheets with deduplication
- **Multi-Gmail Account Support** - Multiple accounts with OAuth, round-robin/weighted rotation
- **Queue-Based Architecture** - NOT direct sending, ensures rate limiting and reliability
- **HTML Email Templates** - Customizable templates with variable injection
- **OpenAI Integration** - Subject line generation and optional light personalization
- **Deliverability Controls** - Per-account limits, timing restrictions, random delays
- **Safety Guards** - Error rate monitoring, automatic account pausing
- **Follow-up System** - Automated follow-ups after 3-4 and 6-8 days

## Project Structure

```
Gmail-Marketing-Automation/
├── config.yaml              # Main configuration file
├── requirements.txt       # Python dependencies
├── .env.example         # Environment variables example
├── accounts/            # Gmail OAuth credentials
├── templates/          # Email templates
├── data/               # SQLite databases
├── logs/               # Log files
├── src/
│   ├── config/         # Configuration loader
│   ├── modules/        # Core modules
│   │   ├── google_sheets.py
│   │   ├── gmail_accounts.py
│   │   ├── email_template.py
│   │   ├── openai_integration.py
│   │   ├── queue_system.py
│   │   └── gmail_api.py
│   ├── services/        # Services
│   │   └── logging_service.py
│   └── pipeline.py     # Main orchestrator
└── README.md
```

## Setup Instructions

### 1. Prerequisites

- Python 3.10+
- Google Cloud Project with Gmail API and Sheets API enabled
- OpenAI API key (optional, for subject lines)
- Redis (optional, for queue storage)

### 2. Installation

```bash
# Clone and navigate to project
cd Gmail-Marketing-Automation

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Google Cloud Setup

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable the following APIs:
   - Google Sheets API
   - Gmail API
3. Create OAuth 2.0 credentials:
   - Go to APIs & Services > Credentials
   - Create Credentials > OAuth client ID
   - Desktop application
4. Download the JSON credentials file

### 4. Gmail Account Setup

Create a credentials file for each Gmail account:

```json
{
  "client_id": "your-client-id.apps.googleusercontent.com",
  "client_secret": "your-client-secret",
  "refresh_token": "your-refresh-token",
  "token_uri": "https://oauth2.googleapis.com/token"
}
```

> Note: You'll need to perform the initial OAuth flow to get refresh tokens for each account.

### 5. Configuration

Copy and configure the configuration files:

```bash
# Copy environment example
cp .env.example .env

# Edit config.yaml with your settings
```

Key configuration options in `config.yaml`:

```yaml
# Google Sheets
sheets:
  credentials_file: "credentials.json"
  spreadsheet_id: "your-spreadsheet-id"
  sheet_range: "Sheet1!A:D"

# Gmail Accounts
gmail_accounts:
  accounts:
    - id: "account_1"
      email: "you@example.com"
      credentials_file: "accounts/gmail_account_1.json"
      daily_limit: 30
      hourly_limit: 8

# Sending Limits
sending_limits:
  max_emails_per_account_day: 30
  send_window_start: 9   # 9 AM UTC
  send_window_end: 17   # 5 PM UTC
  skip_weekends: true

# OpenAI (optional)
openai:
  api_key: "sk-..."
  generate_subject_variations: true
```

### 6. Google Sheets Setup

Your spreadsheet should have these columns:

| No | User Name | email | github_url |
|----|----------|-------|------------|
| 1 | John Doe | john@example.com | https://github.com/johndoe |
| 2 | Jane Smith | jane@example.com | https://github.com/janesmith |

### 7. Email Templates

Place HTML templates in the `templates/` directory:

- `outreach.html` - Main outreach email
- `followup_1.html` - First follow-up (3-4 days)
- `followup_2.html` - Second follow-up (6-8 days)

Use `{{firstName}}` and `{{github_url}}` as variables.

## Usage

### Run a Single Cycle

```bash
python -m src.pipeline
```

### Schedule Automatic Runs

Add to crontab for automatic execution:

```bash
# Run every hour during business hours
0 9-17 * * 1-5 cd /path/to/project && python -m src.pipeline
```

### View Logs

```bash
tail -f logs/email_system.log
```

### Generate Report

```bash
python -c "
import asyncio
from src.pipeline import EmailPipeline

async def report():
    pipeline = EmailPipeline()
    await pipeline.initialize()
    await pipeline.generate_report()
    await pipeline.close()

asyncio.run(report())
"
```

## Architecture

### Data Flow

```
Google Sheets → Lead Loader → Lead Database
                                ↓
                         Lead Validator
                                ↓
                    Email Generator (OpenAI)
                                ↓
                              Queue
                                ↓
                        Sender Workers
                                ↓
                          Gmail API
```

### Queue System

- Leads are read from Google Sheets
- Validated and deduplicated
- Emails generated with template + OpenAI subject
- Added to SQLite queue
- Workers pull from queue with rate limiting
- Sent via Gmail API with OAuth
- Results logged to database

### Rate limiting

- Per-account daily limit (default: 30)
- Per-account hourly limit (default: 8)
- Random delay between emails (3-7 minutes)
- Long pause after batch of 4 emails (15 minutes)
- Send window: 9 AM - 5 PM UTC
- Weekends skipped if configured

## Safety Guards

- Account pause if error rate > 5%
- System pause if global error rate > 10%
- Max 3 consecutive errors before pause
- Duplicate email prevention (30-day window)
- Automatic retry with exponential backoff

## Troubleshooting

### Common Errors

**"No credentials found"**
- Ensure your Google credentials JSON files are in the `accounts/` directory

**"Account cannot send"**
- Account is paused due to errors or rate limits
- Check the logs for reason

**"Rate limit exceeded"**
- Wait for the hourly/daily reset
- Use more accounts

### Logging

Check logs in `logs/email_system.log` for detailed error messages.

## License

MIT License

## Author

## Deployment Platforms

### Recommended: Render (Best for This Project)

**Render** is ideal because:
- Free tier available
- Python 3.11 support
- Persistent disk for SQLite databases
- Scheduled cron jobs
- Easy environment variable management

**Deploy Steps:**
1. Connect your GitHub repository
2. Choose "Web Service"
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `python -m src.pipeline`
5. Add environment variables

### Alternative: Railway

- Good Python support
- Pay-as-you-go pricing
- Automatic deployments

### Alternative: PythonAnywhere

- Free tier available
- Direct Python execution
- Good for testing

---

## Author

Adam Wyrzycki