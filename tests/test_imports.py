def test_imports() -> None:
    """Attempt to import all the modules to test for ModuleNotFoundError."""

    from clean_cddb import checks  # noqa
    from clean_cddb import cleaning_transforms  # noqa
    from clean_cddb import schema  # noqa
    from clean_cddb import utils  # noqa
