from datetime import datetime, timezone


def datetime_now() -> datetime:
    return datetime.now(tz=timezone.utc)
