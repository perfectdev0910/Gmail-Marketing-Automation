"""Email template system module.

This module handles:
- HTML template loading and rendering
- Variable substitution ({{firstName}}, {{github_url}})
- MIME email formatting for Gmail API
"""

import logging
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Email Template
# =============================================================================


class EmailTemplate:
    """Email template with variable substitution."""

    # Default outreach template
    DEFAULT_TEMPLATE = """<html>
  <body>
    <p>Hi <strong>{{firstName}}</strong>,</p>
    <p>I hope you're doing well!</p>

    <p>I'm Adam Wyrzycki, and I manage an agency based in Warsaw, Poland. Our team includes 20 senior-level engineers in the EU, along with 8 collaborators in the US.<br>
    Our US team is mainly responsible for client communication, while all other tasks, like coding, are handled by our EU developers.<br>

    <strong>We're currently looking for a US-based professional who can work part-time with us.</strong><br>

    After reviewing your GitHub profile ({{github_url}}), I was really impressed with your background.<br><br>

    The role would focus exclusively on client communication for one or two hours per day, making use of your native English skills.<br>

    <strong>Here's what we're looking for:</strong><br>
    - US citizenship<br>
    - Development experience.<br><br>

    If this sounds like an opportunity you'd be interested in, I'd love to chat and provide more details.<br><br>

    Best regards,<br>
    Adam Wyrzycki.
    </p>
  </body>
</html>"""

    # Follow-up templates
    FOLLOWUP_1_TEMPLATE = """<html>
  <body>
    <p>Hi <strong>{{firstName}}</strong>,</p>
    <p>Just following up on my earlier email about the part-time role with our agency.</p>
    <p>Thought I'd check if you had a chance to consider this opportunity.</p>
    <p>Let me know if you'd like to chat more!</p>
    <p>Best,<br>Adam</p>
  </body>
</html>"""

    FOLLOWUP_2_TEMPLATE = """<html>
  <body>
    <p>Hi <strong>{{firstName}}</strong>,</p>
    <p>Hope you're having a great week!</p>
    <p>Just one more quick check - would love to chat if you're interested.</p>
    <p>No pressure at all if it's not a fit.</p>
    <p>Best,<br>Adam</p>
  </body>
</html>"""

    def __init__(
        self,
        template_html: Optional[str] = None,
        template_file: Optional[str] = None,
    ):
        self.template_html = template_html or self.DEFAULT_TEMPLATE
        if template_file:
            self.load_from_file(template_file)

    def load_from_file(self, template_file: str) -> None:
        """Load template from file."""
        path = Path(template_file)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                self.template_html = f.read()
                logger.info(f"Loaded template from {template_file}")
        else:
            logger.warning(f"Template file not found: {template_file}")

    def render(
        self,
        first_name: str = "",
        github_url: str = "",
        custom_vars: Optional[dict[str, str]] = None,
    ) -> str:
        """Render template with variable substitution.

        Args:
            first_name: First name of the recipient
            github_url: GitHub profile URL
            custom_vars: Additional custom variables

        Returns:
            Rendered HTML email
        """
        html = self.template_html

        # Replace standard variables
        html = html.replace("{{firstName}}", first_name or "there")
        html = html.replace("{{github_url}}", github_url or "[GitHub profile]")

        # Replace custom variables
        if custom_vars:
            for key, value in custom_vars.items():
                placeholder = f"{{{{{key}}}}}"
                html = html.replace(placeholder, value)

        return html

    def has_variables(self) -> list[str]:
        """Find all variables in template."""
        pattern = r"\{\{(\w+)\}\}"
        matches = re.findall(pattern, self.template_html)
        return list(set(matches))

    def validate_variables(
        self, vars_dict: dict[str, str]
    ) -> tuple[bool, list[str]]:
        """Validate that all required variables are provided.

        Returns:
            Tuple of (is_valid, missing_variables)
        """
        required = set(self.has_variables())
        provided = set(vars_dict.keys())
        missing = required - provided

        return len(missing) == 0, list(missing)


# =============================================================================
# MIME Email Builder
# =============================================================================


class EmailBuilder:
    """Build MIME email messages for Gmail API."""

    def __init__(self, template: Optional[EmailTemplate] = None):
        self.template = template or EmailTemplate()

    def build_email(
        self,
        to_email: str,
        from_email: str,
        subject: str,
        first_name: str = "",
        github_url: str = "",
        custom_vars: Optional[dict[str, str]] = None,
        is_html: bool = True,
    ) -> MIMEMultipart:
        """Build a MIME email message.

        Args:
            to_email: Recipient email address
            from_email: Sender email address
            subject: Email subject line
            first_name: First name for template
            github_url: GitHub profile URL
            custom_vars: Additional template variables
            is_html: Whether to send as HTML

        Returns:
            MIMEMultipart message ready for Gmail API
        """
        # Create message
        message = MIMEMultipart("alternative")
        message["To"] = to_email
        message["From"] = from_email
        message["Subject"] = subject

        # Render HTML body
        html_body = self.template.render(
            first_name=first_name,
            github_url=github_url,
            custom_vars=custom_vars,
        )

        # Create text version (plain text fallback)
        text_body = self._html_to_text(html_body)

        # Attach parts
        part1 = MIMEText(text_body, "plain", "utf-8")
        part2 = MIMEText(html_body, "html", "utf-8")

        message.attach(part1)
        message.attach(part2)

        return message

    def _html_to_text(self, html: str) -> str:
        """Convert HTML to plain text."""
        import re

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", html)

        # Decode HTML entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&amp;", "&")
        text = text.replace("&quot;", '"')

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text)
        text = text.strip()

        return text

    def build_raw_message(
        self,
        to_email: str,
        from_email: str,
        subject: str,
        first_name: str = "",
        github_url: str = "",
        custom_vars: Optional[dict[str, str]] = None,
    ) -> str:
        """Build raw email message for Gmail API.

        Args:
            to_email: Recipient email address
            from_email: Sender email address
            subject: Email subject line
            first_name: First name for template
            github_url: GitHub profile URL
            custom_vars: Additional template variables

        Returns:
            Base64 encoded raw email message
        """
        import base64

        message = self.build_email(
            to_email=to_email,
            from_email=from_email,
            subject=subject,
            first_name=first_name,
            github_url=github_url,
            custom_vars=custom_vars,
        )

        # Encode to base64url format
        raw = message.as_bytes()
        encoded = base64.urlsafe_b64encode(raw).decode()

        return encoded


# =============================================================================
# Template Manager
# =============================================================================


class TemplateManager:
    """Manager for multiple email templates."""

    def __init__(self, templates_dir: str = "templates"):
        self.templates_dir = Path(templates_dir)
        self.templates: dict[str, EmailTemplate] = {}

    def load_template(
        self, name: str, template_file: Optional[str] = None
    ) -> EmailTemplate:
        """Load a template by name."""
        # Check if already loaded
        if name in self.templates:
            return self.templates[name]

        # Try to load from file
        if template_file:
            template_path = self.templates_dir / template_file
        else:
            template_path = self.templates_dir / f"{name}.html"

        if template_path.exists():
            template = EmailTemplate(template_file=str(template_path))
        else:
            # Use default for known templates
            if name == "outreach":
                template = EmailTemplate()
            elif name == "followup_1":
                template = EmailTemplate(
                    template_html=EmailTemplate.FOLLOWUP_1_TEMPLATE
                )
            elif name == "followup_2":
                template = EmailTemplate(
                    template_html=EmailTemplate.FOLLOWUP_2_TEMPLATE
                )
            else:
                logger.warning(f"Template {name} not found, using default")
                template = EmailTemplate()

        self.templates[name] = template
        return template

    def get_template(self, name: str) -> EmailTemplate:
        """Get a template by name."""
        if name not in self.templates:
            return self.load_template(name)
        return self.templates[name]

    def render_with_template(
        self,
        template_name: str,
        to_email: str,
        from_email: str,
        subject: str,
        first_name: str = "",
        github_url: str = "",
        custom_vars: Optional[dict[str, str]] = None,
    ) -> str:
        """Render an email with a specific template."""
        template = self.get_template(template_name)
        builder = EmailBuilder(template)

        return builder.build_raw_message(
            to_email=to_email,
            from_email=from_email,
            subject=subject,
            first_name=first_name,
            github_url=github_url,
            custom_vars=custom_vars,
        )