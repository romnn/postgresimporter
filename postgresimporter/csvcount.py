import argparse
import json
import logging
import multiprocessing
import re
from pathlib import Path

from . import exec, utils

logger = logging.getLogger("csvcount")


def _count_csv_entries(dump_files):
    counts = dict()
    if isinstance(dump_files, str):
        dump_files = Path(dump_files)
    if isinstance(dump_files, Path):
        dump_files = dump_files.rglob("*.csv") if dump_files.is_dir() else dump_files
    if not isinstance(dump_files, list):
        dump_files = [dump_files]
    for file in dump_files:
        counts[file] = utils.count_csv_entries(file)
    return counts


async def count_csv_entries(files, precise=False):
    if len(files) < 1:
        logger.info("No csv files to count entries for")
        return
    [logger.info(f"Counting entries of {str(file.absolute())}") for file in files]
    results = dict()
    if precise:
        _results = await exec.run_simultaneously(
            [
                ("python", ["-m", "postgresimporter.csvcount", str(file.absolute())],)
                for file in files
            ],
            max_concurrency=max(1, int(multiprocessing.cpu_count() / 2)),
        )
        for r in _results:
            results.update(json.loads(r))
    else:
        _results = await exec.run_simultaneously(
            [("wc", ["-l", str(file.absolute())]) for file in files],
            max_concurrency=max(1, int(multiprocessing.cpu_count() / 2)),
        )
        for r in _results:
            components = re.search(r"^(\d+) (.*)$", r.decode("utf-8"))
            results.update({components.group(2): int(components.group(1))})
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source",
        type=lambda x: utils.valid_dir_or_file(parser, x),
        help="database dump source directory",
    )

    args, unknown = parser.parse_known_args()
    print(json.dumps(_count_csv_entries(args.source)))
