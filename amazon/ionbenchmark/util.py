TOOL_VERSION = '1.0.0'


def str_to_bool(v):
    if isinstance(v, str):
        return v.lower() in ("true", "1")
    return None


def format_percentage(v):
    return "{:.2%}".format(v)


def format_decimal(v):
    return "{:.2e}".format(v)

