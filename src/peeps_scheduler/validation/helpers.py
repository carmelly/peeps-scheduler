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
    seen = set()
    duplicates = set()

    for item in items:
        value = key(item) if key else item
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    if duplicates:
        raise ValueError(f"{msg}: {', '.join(str(d) for d in duplicates)}")
