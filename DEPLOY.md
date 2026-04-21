# Deployment Guide

This guide covers deploying the Email Marketing Automation system to **Render** (recommended).

## Quick Deploy to Render

### Step 1: Prepare Your Repository

Push your code to GitHub:
```bash
git add .
git commit -m "Deploy-ready version"
git push origin email-automation-system
```

### Step 2: Create Render Account

1. Go to [render.com](https://render.com)
2. Sign up with GitHub
3. Click "New +" → "Web Service"

### Step 3: Configure Deployment

| Setting | Value |
|---------|-------|
| Name | email-automation |
| Environment | Python 3.11 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python -m src.pipeline` |

### Step 4: Add Environment Variables

In Render dashboard, go to "Environment" tab and add:

```
OPENAI_API_KEY=sk-your-openai-key
SPREADSHEET_ID=your-spreadsheet-id
```

### Step 5: Add Secrets (Via Files)

For credentials files, you'll need to add them via Render's "Secret Files" feature:

1. Go to your Web Service → "Files" → "Secret Files"
2. Upload:
   - `credentials.json` - Google Sheets API credentials
   - `accounts/gmail_account_1.json` - Gmail OAuth credentials
   - `accounts/gmail_account_2.json` - (if second account)

### Step 6: Configure config.yaml

Update `config.yaml` with your settings:

```yaml
sheets:
  spreadsheet_id: "YOUR_SPREADSHEET_ID"
  sheet_range: "Sheet1!A:E"  # 5 columns: No, Name, Email, GitHub, Status

gmail_accounts:
  accounts:
    - id: "account_1"
      email: "your-email@gmail.com"
      credentials_file: "accounts/gmail_account_1.json"
```

### Step 7: Prepare Google Sheets

Your spreadsheet should have these columns (Sheet1 with header row):

| A | B | C | D | E |
|---|---|---|---|---|
| No | User Name | email | github_url | status |
| 1 | Hadley Wickham | h.wickham@gmail.com | https://github.com/hadley | pending |
| 2 | Steve Klabnik | steve@steveklabnik.com | https://github.com/steveklabnik | pending |
| 3 | Dave Rupert | rupato@gmail.com | https://github.com/davatron5000 | pending |

**Status Column (E) values:**
- `pending` - New lead, ready to process (default)
- `queued` - Loaded, waiting in queue
- `processing` - Being processed/sending
- `sent` - Email sent successfully
- `failed` - Sending failed
- `skipped` - Skipped (invalid data, etc.)

**Important:** Set Column E to "pending" (or leave blank) for new leads. The system automatically updates this column as leads are processed. This prevents duplicate processing when redeploying.

### Step 7: Deploy

Click "Create Web Service"

## Scheduled Jobs (Alternative)

Instead of running continuously, you can run on a schedule:

### Option A: Render Cron

1. Create a new "Cron Job" in Render dashboard
2. Command: `python -m src.pipeline`
3. Schedule: `0 9-17 * * 1-5` (every hour, weekdays 9-5 UTC)

### Option B: External Scheduler

Use a free cron service to hit your endpoint:

```bash
# Create a simple health check endpoint
# Then use crontab -e or cron-job.org
```

## Google Sheets Format

Your spreadsheet should have 5 columns with a header row (Sheet1):

| A | B | C | D | E |
|---|---|---|---|---|
| No | User Name | email | github_url | status |
| 1 | Hadley Wickham | h.wickham@gmail.com | https://github.com/hadley | pending |
| 2 | Steve Klabnik | steve@steveklabnik.com | https://github.com/steveklabnik | pending |
| 3 | Dave Rupert | rupato@gmail.com | https://github.com/davatron5000 | sent |

**Status values:**
- `pending` - New leads to process (set this for new data)
- `queued` - In queue waiting to send
- `processing` - Currently sending
- `sent` - Successfully sent
- `failed` - Failed to send
- `skipped` - Skipped

**How it prevents duplicates:**
The system only reads rows with status `pending`. When you redeploy, rows already marked as `sent` or `queued` are skipped, so no duplicates are sent.

## Gmail OAuth Setup

### Step 1: Enable Gmail API

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create project → Enable "Gmail API"
3. Create OAuth credentials (Desktop application)
4. Download JSON

### Step 2: Get Refresh Token

Run this locally to get a refresh token:

```python
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
creds = flow.run_localserver(port=0)
print(creds.to_json())
```

### Step 3: Create Account File

Create `accounts/gmail_account_1.json`:

```json
{
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET", 
  "refresh_token": "YOUR_REFRESH_TOKEN",
  "token_uri": "https://oauth2.googleapis.com/token"
}
```

## Troubleshooting

### " credential file not found"

Make sure credentials files are added to Render's Secret Files.

### " rate limit exceeded"

Wait for the hourly reset. Add more Gmail accounts to rotate.

### " OpenAI API error"

Check your API key in environment variables.

### Database error

Ensure you're using the Free tier's persistent disk. For production, consider upgrading.