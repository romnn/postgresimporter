import argparse
import os

from . import utils


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "sources",
        type=lambda x: utils.valid_dir_or_file(parser, x, extensions=[".zip", ".csv"]),
        action="append",
        nargs="+",
        help="database dump source directory",
    )
    parser.add_argument(
        "--all",
        default=False,
        action="store_true",
        help="whether all files should be unzipped and imported again",
    )

    # Stages
    parser.add_argument(
        "--disable-unzip",
        dest="disable_unzip",
        default=False,
        action="store_true",
        help="whether to skip unzipping compressed files in source directory",
    )
    parser.add_argument(
        "--disable-import",
        dest="disable_import",
        default=False,
        action="store_true",
        help="whether to skip importing csv files into database",
    )
    parser.add_argument(
        "--combine-tables",
        dest="combine_tables",
        default=False,
        action="store_true",
        help="whether to combine imported csv file tables into one table named by prefix",
    )
    parser.add_argument(
        "--disable-check",
        dest="disable_check",
        default=False,
        action="store_true",
        help="whether to skip checking csv row count and database row count after loading",
    )

    # Filtering
    parser.add_argument(
        "--exclude-regex",
        dest="exclude_regex",
        default=None,
        help="regex for files to be excluded from import and unzip",
    )

    # Hooks
    parser.add_argument(
        "--post-load",
        dest="post_load",
        type=lambda x: utils.valid_dir_or_file(parser, x, extensions=[".sql"]),
        action="append",
        nargs="+",
        help="optional post load script to execute after import",
    )
    parser.add_argument(
        "--pre-load",
        dest="pre_load",
        type=lambda x: utils.valid_dir_or_file(parser, x, extensions=[".sql"]),
        action="append",
        nargs="+",
        help="optional pre load script to execute before import",
    )

    # Database connection options
    default_db_name = "postgres"
    default_db_host = "localhost"
    default_db_port = 5432
    default_db_user = "postgres"
    parser.add_argument(
        "--db-name",
        type=str,
        dest="db_name",
        help=f'database name (default "{default_db_name}")',
    )
    parser.add_argument(
        "--db-host",
        type=str,
        dest="db_host",
        help=f'database host (default "{default_db_host}")',
    )
    parser.add_argument(
        "--db-port",
        type=int,
        dest="db_port",
        help=f'database port (default "{default_db_port}")',
    )
    parser.add_argument(
        "--db-user",
        type=str,
        dest="db_user",
        help=f'database user (default "{default_db_user}"")',
    )
    parser.add_argument(
        "--db-password", type=str, dest="db_password", help="database password"
    )

    parser.add_argument(
        "--log-level",
        type=lambda x: utils.valid_log_level(parser, x),
        dest="log_level",
        default="INFO",
        help="log level (%s)" % str(", ").join(utils.log_levels),
    )

    args, unknown = parser.parse_known_args()

    # Check for environment variables with lower precedence
    args.db_name = args.db_name or os.environ.get("DB_NAME") or default_db_name
    args.db_host = args.db_host or os.environ.get("DB_HOST") or default_db_host
    args.db_port = args.db_port or os.environ.get("DB_PORT") or default_db_port
    args.db_user = args.db_user or os.environ.get("DB_USER") or default_db_user
    args.db_password = args.db_password or os.environ.get("DB_PASSWORD")
    return args, unknown
