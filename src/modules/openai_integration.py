"""OpenAI integration module.

This module handles:
- Subject line generation (3-5 variations)
- Optional light personalization
- Strict spam language prevention
"""

import logging
import random
import re
from typing import Any, Optional

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


# =============================================================================
# Subject Line Generator
# =============================================================================


SUBJECT_PROMPTS = {
    "professional": (
        "Generate a professional, concise email subject line for a job outreach. "
        "Keep it under 50 characters. No hype or spam language."
    ),
    "neutral": (
        "Generate a neutral, curious email subject line that invites a response. "
        "Keep it under 50 characters. No sales language."
    ),
    "curiosity": (
        "Generate a curiosity-inducing subject line that makes the recipient want to open. "
        "Keep it under 50 characters. Don't be pushy."
    ),
}


class SubjectLineGenerator:
    """Generate subject line variations using OpenAI."""

    def __init__(self, client: AsyncOpenAI, model: str = "gpt-4"):
        self.client = client
        self.model = model

    async def generate_variations(
        self,
        first_name: str = "",
        github_url: str = "",
        count: int = 3,
        styles: Optional[list[str]] = None,
    ) -> list[str]:
        """Generate subject line variations.

        Args:
            first_name: First name of the recipient
            github_url: GitHub profile URL (optional)
            count: Number of variations to generate
            styles: Styles to use (professional, neutral, curiosity)

        Returns:
            List of subject line variations
        """
        if styles is None:
            styles = ["professional", "neutral", "curiosity"]

        # Build context
        context_parts = []
        if first_name:
            context_parts.append(f"Recipient: {first_name}")
        if github_url:
            context_parts.append(f"GitHub: {github_url}")

        context = ", ".join(context_parts) if context_parts else "professional outreach"

        system_prompt = (
            "You are an expert email marketer. Generate clean, professional subject lines "
            "that get opens without being spammy. DO NOT use: !, ALL CAPS, aggressive words, "
            "or anything that triggers spam filters. Keep it natural and respectful."
        )

        variations = []

        for style in styles[:count]:
            try:
                user_prompt = (
                    f"Context: {context}\n\n"
                    f"Generate a {style} style subject line for a cold outreach about "
                    f"a part-time US-based client communication role. "
                    f"The recipient is a developer. Keep it under 50 characters."
                )

                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.7,
                    max_tokens=60,
                )

                subject = response.choices[0].message.content.strip()

                # Clean up subject
                subject = self._clean_subject(subject)

                if subject and len(subject) <= 60:
                    variations.append(subject)

            except Exception as e:
                logger.error(f"Error generating subject: {e}")
                continue

        # Add fallback variations if needed
        if len(variations) < count:
            variations.extend(self._get_fallback_subjects(count - len(variations)))

        return variations[:count]

    def _clean_subject(self, subject: str) -> str:
        """Clean subject line of spam triggers."""
        # Remove quotes if present
        subject = subject.strip().strip('"').strip("'")

        # Remove spam triggers
        spam_patterns = [
            r"!+",
            r"\b(free|act now|limited|time sensitive|urgent|exclusive)\b",
            r"\$\d+",
            r"\d+%\s+off",
        ]

        for pattern in spam_patterns:
            subject = re.sub(pattern, "", subject, flags=re.IGNORECASE)

        # Clean up whitespace
        subject = re.sub(r"\s+", " ", subject).strip()

        return subject

    def _get_fallback_subjects(self, count: int) -> list[str]:
        """Get safe fallback subjects."""
        fallbacks = [
            "Quick question about your availability",
            "Part-time opportunity for developers",
            "Following up on your GitHub profile",
            "Part-time US client communication role",
            "Developer part-time opportunity",
        ]
        return random.sample(fallbacks, min(count, len(fallbacks)))

    async def select_best_subject(
        self, first_name: str, github_url: str
    ) -> str:
        """Select the best subject line variation."""
        variations = await self.generate_variations(
            first_name=first_name, github_url=github_url, count=3
        )

        if not variations:
            return "Part-time opportunity for developers"

        # Select based on some heuristics
        for subject in variations:
            # Prefer subjects with personalization
            if first_name.lower() in subject.lower():
                return subject

        # Default to first variation
        return variations[0]


# =============================================================================
# Light Personalizer
# =============================================================================


class LightPersonalizer:
    """Light personalizer using OpenAI for minor improvements."""

    def __init__(self, client: AsyncOpenAI, model: str = "gpt-4"):
        self.client = client
        self.model = model

    # List of words to avoid (spam triggers)
    SPAM_WORDS = {
        "urgent",
        "act now",
        "limited time",
        "free money",
        "guaranteed",
        "no risk",
        "exclusive deal",
        "act fast",
        "don't miss",
        "winner",
        "congratulations",
        "prize",
        "cash bonus",
        "make $",
        "earn $",
        "100% free",
    }

    async def improve_tone(
        self, original_text: str, first_name: str = ""
    ) -> str:
        """Improve the tone of the email slightly.

        Args:
            original_text: Original email text
            first_name: Recipient's first name

        Returns:
            Improved text (minimal changes)
        """
        # This should NOT rewrite the full email - only minor improvements
        system_prompt = (
            "You are an email tone assistant. You make MINIMAL improvements to email drafts. "
            "Your job is only to: "
            "1. Improve the opening line slightly for better warmth "
            "2. Keep the original message and intent completely intact "
            "3. DO NOT rewrite the email or add new content "
            "4. Keep it short - single sentence improvement max "
            "DO NOT use any spam trigger words."
        )

        user_prompt = (
            f"Improve only the opening greeting for: {first_name or 'there'}\n\n"
            f"Original: {original_text[:200]}\n\n"
            f"Respond with just the improved opening line, nothing else."
        )

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=100,
            )

            improved = response.choices[0].message.content.strip()

            # Validate
            if self._is_spam_free(improved):
                return improved

        except Exception as e:
            logger.error(f"Error improving tone: {e}")

        return original_text

    async def improve_intro(
        self, original_intro: str, first_name: str, github_url: str = ""
    ) -> str:
        """Improve the intro line (max 1 line).

        Args:
            original_intro: Original intro line
            first_name: Recipient's first name
            github_url: GitHub profile URL (optional)

        Returns:
            Improved intro (single sentence)
        """
        system_prompt = (
            "You improve email intros slightly. Keep it to ONE SHORT sentence. "
            "Make it warmer and more genuine. DO NOT overdo it."
        )

        context = f"Name: {first_name}"
        if github_url:
            context += f", GitHub: {github_url}"

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"Improve: {original_intro}\n\nContext: {context}",
                    },
                ],
                temperature=0.3,
                max_tokens=50,
            )

            improved = response.choices[0].message.content.strip()

            # Validate length
            if len(improved) <= 100 and self._is_spam_free(improved):
                return improved

        except Exception as e:
            logger.error(f"Error improving intro: {e}")

        return original_intro

    def _is_spam_free(self, text: str) -> bool:
        """Check if text is free of spam triggers."""
        text_lower = text.lower()

        for spam_word in self.SPAM_WORDS:
            if spam_word in text_lower:
                return False

        return True


# =============================================================================
# OpenAI Service
# =============================================================================


class EmailBodyGenerator:
    """Generate complete email body using OpenAI."""

    # Spam words to avoid
    SPAM_TRIGGERS = {
        "urgent", "act now", "limited time", "free money", "guaranteed",
        "no risk", "exclusive deal", "act fast", "don't miss", "winner",
        "congratulations", "prize", "cash bonus", "make $", "earn $", "100% free",
    }

    def __init__(self, client: AsyncOpenAI, model: str = "gpt-4"):
        self.client = client
        self.model = model

    async def generate_email(
        self,
        first_name: str,
        github_url: str = "",
        context: str = "part-time US-based client communication role for a Warsaw-based software agency",
    ) -> tuple[str, str]:
        """Generate personalized email subject and body.

        Args:
            first_name: Recipient's first name
            github_url: GitHub profile URL (optional)
            context: What the email is about

        Returns:
            Tuple of (subject, body_html)
        """
        system_prompt = """You are an expert cold email writer. You write personalized, 
professional outreach emails that feel genuine and not spammy.

Rules:
1. Keep emails SHORT - 3-5 short paragraphs max
2. Reference the person's GitHub profile if available
3. Be specific about the opportunity
4. Use a casual, friendly tone
5. NO sales hype, ALL CAPS, or spam triggers
6. Always include a soft call-to-action
7. Sign off as 'Adam Wyrzycki' from 'Warsaw, Poland'
8. Output ONLY valid HTML with <p> tags, no markdown code blocks"""

        user_prompt = f"""Write a cold outreach email for {first_name}.

GitHub profile: {github_url if github_url else 'Not provided'}
Opportunity: {context}

Format:
- Subject: [catchy but professional subject line]
- Body: [personalized HTML email with <p> tags]

Make it personal by mentioning something that would make sense for a developer."""
        body_text = ""
        subject = "Quick question about your availability"

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.8,
                max_tokens=800,
            )

            content = response.choices[0].message.content.strip()

            # Parse subject and body
            if "Subject:" in content:
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    if line.startswith("Subject:"):
                        subject = line.replace("Subject:", "").strip()
                        body_text = "\n".join(lines[i+1:]).strip()
                        break
            else:
                body_text = content

            # Clean up and convert to HTML
            body_html = self._convert_to_html(body_text)

            # Validate no spam triggers
            if not self._is_spam_free(body_html):
                # Use fallback if spam detected
                subject, body_html = self._get_fallback(first_name, github_url)

        except Exception as e:
            logger.error(f"Error generating email: {e}")
            subject, body_html = self._get_fallback(first_name, github_url)

        return subject, body_html

    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to HTML paragraphs."""
        import re

        # Split into paragraphs
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

        html_parts = []
        for para in paragraphs:
            # Clean up the paragraph
            para = re.sub(r'\s+', ' ', para).strip()
            if para:
                html_parts.append(f"<p>{para}</p>")

        return "\n".join(html_parts)

    def _is_spam_free(self, text: str) -> bool:
        """Check for spam triggers."""
        text_lower = text.lower()
        for word in self.SPAM_TRIGGERS:
            if word in text_lower:
                return False
        return True

    def _get_fallback(self, first_name: str, github_url: str) -> tuple[str, str]:
        """Get fallback email template."""
        subject = "Quick question about your availability"

        github_ref = f"After reviewing your GitHub profile ({github_url}), " if github_url else ""

        body = f"""<p>Hi {first_name},</p>
<p>I hope you're doing well!</p>
<p>{github_ref}I was impressed with your work.</p>
<p>I'm Adam Wyrzycki, and I manage a software agency in Warsaw, Poland. We're looking for a US-based developer for part-time client communication work (1-2 hours/day).</p>
<p>If you're interested, I'd love to chat more.</p>
<p>Best,<br>Adam</p>"""

        return subject, body


# =============================================================================
# Service Alias (for backward compatibility)
# =============================================================================


class OpenAIService:
    """Main service for OpenAI integrations."""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4",
        temperature: float = 0.7,
    ):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self.temperature = temperature

        # Initialize components
        self.subject_generator = SubjectLineGenerator(self.client, model)
        self.personalizer = LightPersonalizer(self.client, model)
        self.body_generator = EmailBodyGenerator(self.client, model)

    async def generate_subject(
        self,
        first_name: str = "",
        github_url: str = "",
    ) -> str:
        """Generate a single subject line."""
        return await self.subject_generator.select_best_subject(
            first_name=first_name, github_url=github_url
        )

    async def generate_subjects(
        self,
        first_name: str = "",
        github_url: str = "",
        count: int = 3,
    ) -> list[str]:
        """Generate multiple subject lines."""
        return await self.subject_generator.generate_variations(
            first_name=first_name, github_url=github_url, count=count
        )

    async def generate_email(
        self,
        first_name: str,
        github_url: str = "",
    ) -> tuple[str, str]:
        """Generate complete email (subject + body).

        Args:
            first_name: Recipient's first name
            github_url: GitHub profile URL

        Returns:
            Tuple of (subject, body_html)
        """
        return await self.body_generator.generate_email(
            first_name=first_name, github_url=github_url
        )

    async def personalize(
        self,
        email_text: str,
        first_name: str = "",
        github_url: str = "",
    ) -> str:
        """Apply light personalization to email."""
        return await self.personalizer.improve_intro(
            email_text, first_name=first_name, github_url=github_url
        )

    async def close(self) -> None:
        """Close the client connection."""
        await self.client.close()