from typing import Any

import pytest

from clean_cddb.cleaning_transforms import (
    clean_value_standardize_various_artist,
    clean_value_try_to_fix_encoding_errors,
)


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Test cases for clean_value_standardize_various_artist() function
        ("John Doe", "John Doe"),
        ("Various Artists", "Various"),
        ("Various Artist", "Various"),
        ("various", "Various"),
        ("VaRiOuS aRtIsT", "Various"),
        ("Various", "Various"),
        ("", ""),
        (123, "123"),
        ("   Led Zeppelin   ", "Led Zeppelin"),
    ],
)
def test_clean_value_standardize_various_artist(
    input_value: Any, expected_output: str
) -> None:
    assert clean_value_standardize_various_artist(input_value) == expected_output


@pytest.mark.parametrize(
    "input_value, expected_output",
    # Test cases to test that bad encodings are getting fixed
    [
        ("NguyÃªn LÃª", "Nguyên Lê"),
        ("Olle LjungstrÃ¶m", "Olle Ljungström"),
        ("Camerata FlorianÃ³polis", "Camerata Florianópolis"),
        ("LeÃ¦ther Strip", "Leæther Strip"),
        ("BÃ¼rger Lars Dietrich", "Bürger Lars Dietrich"),
        ("MÃ¶gel", "Mögel"),
    ],
)
def test_clean_value_try_to_fix_encoding_errors(
    input_value: Any, expected_output: str
) -> None:
    assert clean_value_try_to_fix_encoding_errors(input_value) == expected_output


if __name__ == "__main__":
    pytest.main()
