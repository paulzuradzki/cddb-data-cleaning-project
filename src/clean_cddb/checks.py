from typing import Any
import re


def check_col_has_valid_characters(x: Any) -> bool:
    """Check for *possibly* invalid symbols."""

    # consider NaNs and floats to be invalid
    if not isinstance(x, str):
        return False

    invalid_symbols = set(
        "\^z¤¦©¬®¯°±²³´µ¶¸¹º»¼½¾¿ÀÂÃÄÅÆÇÈÌÕÖÜàâäåçèéïð÷øùû˜ѼҸ€中俊劇四団季雅�"
    )
    for char in x:
        if char in invalid_symbols:
            return False

    chinese_pattern = re.compile(r"[\u4e00-\u9fff]")  # CJK Unified Ideographs
    japanese_pattern = re.compile(
        r"[\u3040-\u30ff\u31f0-\u31ff\u3200-\u9faf]"
    )  # Hiragana, Katakana, CJK Unified Ideographs Extension A
    has_chinese = bool(chinese_pattern.search(x))
    has_japanese = bool(japanese_pattern.search(x))

    if has_chinese or has_japanese:
        return False

    return True


def check_artist_is_valid(x: Any) -> bool:
    """Check for invalid artist values."""

    various_artist_pattern = r"\b(various|various artist(s)?|var)\b"
    try:
        # match on variations of "various", "various artist",
        # "various artists", and "var"; case-insensitive
        # also match 2 or more question marks like "????" or "?? ??"
        if _match := re.search(various_artist_pattern, re.escape(x), re.IGNORECASE):
            if x != "Various":
                return False

        if "??" in x:
            return False
    except Exception:
        return False

    return True


def check_category_is_valid(x: Any) -> bool:
    """Check for invalid categories."""

    if x in [
        "blues",
        "classical",
        "country",
        "folk",
        "jazz",
        "misc",
        "newage",
        "reggae",
        "rock",
        "soundtrack",
        "N/A",
        # "REJECT_ROW",
    ]:
        return True
    else:
        return False


def check_genre_is_valid(x: Any) -> bool:
    """Check for invalid genres."""
    try:
        if "--" in x:
            return False
    except Exception:
        return False
    else:
        return True


def check_year_range_is_valid(x: Any) -> bool:
    """Check that year is between 1950 and 2030."""

    try:
        int(x)
    except Exception:
        return False

    if int(x) > 1950 and int(x) < 2030:
        return True
    else:
        return False


def check_year_is_numeric(x: Any) -> bool:
    """Check if year is numeric."""

    try:
        if not str(int(x)).isnumeric():
            return False
    except Exception:
        return False
    return True


def check_track_has_numeric_prefix(x: Any) -> bool:
    """Check for tracks *possibly* using numeric prefix."""
    for keyword in ["disk", "track", "title", "01"]:
        if keyword in x.lower():
            return False
    return True

def check_id_six_digit_starting_one(x: Any) -> bool:
    if int(x) not in range(100000, 200000):
        return False
    return True
