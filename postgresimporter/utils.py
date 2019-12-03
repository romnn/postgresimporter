import csv
import re
import unicodedata
from pathlib import Path

import chardet
import pkg_resources

log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "FATAL"]


def count_csv_entries(file):
    with open(
        file, encoding=chardet.detect(open(file, "rb").read())["encoding"]
    ) as csv_file:
        reader = csv.reader(csv_file, delimiter=" ", quotechar="|")
        return sum(1 for _ in reader)


def files_in(dir_or_file, of_type=None):
    _path = Path(dir_or_file)
    return (
        [dir_or_file]
        if _path.is_file()
        else (_path.iterdir() if not of_type else _path.rglob("*." + of_type))
    )


def merge_dicts(l):
    merged = dict()
    [merged.update(ll) for ll in l]
    return merged


def _valid(_parser, arg, must_match, error_message):
    if must_match(arg):
        return arg
    _parser.error(error_message)


def valid_dir_or_file(_parser, arg, extensions=None):
    return _valid(
        _parser,
        arg,
        must_match=lambda x: (
            Path(arg).exists()
            and (
                Path(x).is_dir()
                or (
                    Path(x).is_file()
                    and (extensions is None or Path(x).suffix.lower() in extensions)
                )
            )
        ),
        error_message="%s is not a directory or existing file" % arg,
    )


def valid_log_level(_parser, arg):
    return _valid(
        _parser,
        arg,
        must_match=lambda x: x.upper() in log_levels,
        error_message="%s is not a valid log level. Must be one of %s"
        % (arg, str(", ").join(log_levels)),
    )


def to_filename(_title: str) -> str:
    title = str(_title).replace(chr(223), "ss")
    title = str(title).replace(chr(228), "ae")
    title = str(title).replace(chr(246), "oe")
    title = str(title).replace(chr(252), "ue")
    title = str(title).replace(".", "_")
    title_bytes = str(unicodedata.normalize("NFKD", title)).encode(
        "ascii", errors="ignore"
    )  # type: bytes
    title = title_bytes.decode("utf-8")
    title = re.sub(r"[^\w\s-]", "", title).strip()
    title = re.sub(r"[-\s]+", "_", title)
    return title


def table_name_for_path(file_path):
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    filename = to_filename(file_path.stem)
    return filename.split("_")[0]


def to_cli_options(options: dict):
    cli_options = list()
    for key, value in options.items():
        cli_options += [f"--{key}", value] if value else []
    return cli_options


def packaged(module, file):
    try:
        return Path(pkg_resources.resource_filename(module, file))
    except ModuleNotFoundError:
        return Path() / file
