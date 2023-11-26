def human_readable_bytes(size: int | float) -> str:
    """Return a human readable string representation of bytes"""
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size = size / 1024.0
    return f"{size:.2f} PiB"
