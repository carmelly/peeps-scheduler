import re


def normalize_email_for_match(email: str) -> str:
    """
    Normalize email for matching.

    - Lowercase and trim whitespace.
    - For Gmail addresses, remove dots from the local part.

    Examples:
        "John.Smith@Gmail.COM" -> "johnsmith@gmail.com"
        "user@example.com" -> "user@example.com"
    """
    if not email:
        return ""

    normalized = email.strip().lower()
    if normalized.endswith("@gmail.com"):
        local, domain = normalized.rsplit("@", 1)
        local = local.replace(".", "")
        return f"{local}@{domain}"
    return normalized


def validate_unique(items, key=None, msg="duplicate value"):
    values = [key(item) if key else item for item in items]
    if len(values) != len(set(values)):
        raise ValueError(msg)


def normalize_topic(topic: str) -> str:
    """
    Normalize topic string for duplicate detection.
    Lowercase, remove parentheticals, and trim whitespace.
    """
    topic = " ".join(re.sub(r"\([^)]*\)", "", topic).split()).strip().lower()
    return topic
