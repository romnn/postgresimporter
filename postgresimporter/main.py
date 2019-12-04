import asyncio
import concurrent.futures
import itertools
import json
import logging
import multiprocessing
import os
import re
import signal
from pathlib import Path

from prettytable import PrettyTable

from . import cli, csvcount, exec, utils

try:
    from progressbar import ProgressBar, UnknownLength

except ImportError:
    UnknownLength = 0

    class ProgressBar:
        def __init__(self, *args, **kwargs):
            pass

        def update(self, *args, **kwargs):
            pass


logger = logging.getLogger("loader")


class Loader:
    args = None
    bar = ProgressBar(max_value=UnknownLength)
    zip_total = 0
    zip_done = 0
    load_total = 0
    load_done = dict()
    tables_created = list()
    table_lock = asyncio.Lock()

    executor = concurrent.futures.ProcessPoolExecutor(
        max_workers=max(1, multiprocessing.cpu_count() - 1)
    )
    queue = asyncio.PriorityQueue()

    def reset(self):
        self.zip_total = self.zip_done = self.load_total = 0
        self.load_done = dict()

    def __init__(self, args, progress=True):
        self.progress = progress
        self.args = args

    async def check_progress(self, output_handler=None, completion_handler=None):
        if not self.progress:
            return
        try:
            while True:
                try:
                    priority, item = await asyncio.wait_for(
                        self.queue.get(), timeout=10.0
                    )
                    logger.debug("Enqueued priority: %s" % priority)
                except asyncio.TimeoutError:
                    logger.warning("Queue is empty (received timeout)")
                    return
                logger.debug(f"Queue size: {self.queue.qsize()}")
                if output_handler:
                    # Handle output (e.g. scrape progress)
                    priority += await output_handler(item.process, item.cmd) or 0
                if item.process.returncode is not None:
                    # Wait for task to finish
                    logger.debug(f"Waiting for {item.cmd} to terminate")
                    _, stderr = await item.process.communicate()
                    logger.debug(f"{item.cmd} terminated")
                    self.queue.task_done()
                    if completion_handler:
                        await completion_handler(item.process, item.cmd, stderr)
                    logger.debug(f"{item.cmd} completion was handled")
                else:
                    # Enqueue again for later
                    self.queue.put_nowait(
                        (priority, exec.PrioritizedItem(item.process, item.cmd))
                    )
                    logger.debug(
                        f"{item.cmd} was queued again with priority {priority}"
                    )
                await self.update_progress()
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            while True:
                try:
                    process, cmd = await asyncio.wait_for(
                        self.queue.get(), timeout=10.0
                    )
                    process.terminate()
                    await asyncio.wait(process)
                    self.queue.task_done()
                except asyncio.TimeoutError:
                    pass

    async def update_progress(self):
        self.bar.max_value = self.zip_total + self.load_total
        self.bar.update(
            min(
                self.bar.max_value,
                max(
                    0,
                    round(
                        self.zip_done
                        + sum(l.get("percent", 0) for l in self.load_done.values()),
                        ndigits=2,
                    ),
                ),
            )
        )

    @property
    def db_options(self):
        options = {
            "dbname": self.args.db_name or os.environ.get("DB_NAME"),
            "host": self.args.db_host or os.environ.get("DB_HOST"),
            "port": self.args.db_port or os.environ.get("DB_PORT"),
            "username": self.args.db_user or os.environ.get("DB_USER"),
            "pass": self.args.db_password or os.environ.get("DB_PASSWORD"),
        }
        return {k: str(v) for k, v in options.items() if v is not None}

    @property
    def sql_db_options(self):
        _db_options = self.db_options
        db_options = {
            "host": _db_options.get("host"),
            "port": _db_options.get("port"),
            "user": _db_options.get("username"),
            "password": _db_options.get("pass"),
        }
        return {k: v for k, v in db_options.items() if v is not None}

    async def step1_unzip(self, data_dirs):
        zip_files = list()
        for data_dir in data_dirs:
            zip_files += (
                data_dir.rglob("*.zip")
                if data_dir.is_dir()
                else ([data_dir] if data_dir.suffix == ".zip" else [])
            )
        zip_files = list(set(zip_files))  # Remove duplicates
        unzipped_files = [
            (zip_file, zip_file.with_name(zip_file.stem)) for zip_file in zip_files
        ]
        not_yet_unzipped = [
            (zip_file, unzipped)
            for zip_file, unzipped in unzipped_files
            if not unzipped.exists()
        ]
        if self.args.disable_unzip and not self.args.all:
            logger.info(
                f"Skipping unzipping of {len(not_yet_unzipped)} not yet unzipped files ({len(zip_files)} total)"
            )
        else:
            [unzipped.mkdir() for _, unzipped in not_yet_unzipped]
            await self.unzip(
                sorted(not_yet_unzipped if not self.args.all else unzipped_files)
            )

    async def step2_import(self, data_dirs):
        dump_files = list()
        for data_dir in data_dirs:
            dump_files += (
                data_dir.rglob("*.csv")
                if data_dir.is_dir()
                else ([data_dir] if data_dir.suffix == ".csv" else [])
            )
        dump_files = list(set(dump_files))  # Remove duplicates
        if self.args.exclude_regex:
            dump_files = [
                file
                for file in dump_files
                if not re.match(re.compile(self.args.exclude_regex), file.stem)
            ]
        tables = {utils.table_name_for_path(file) for file in dump_files}
        table_csv_files = {
            table_name: [
                filename.absolute()
                for filename in dump_files
                if utils.table_name_for_path(filename) == table_name
            ]
            for table_name in tables
        }

        # Import
        if self.args.disable_import and not self.args.all:
            logger.info(f"Skipping importing of {len(dump_files)} csv files")
        else:
            await self.import_data(table_csv_files)

        # Declare a default set of packaged functions
        await exec.exec_sql(
            self.sql_db_options,
            script=utils.packaged("postgresimporter", "hooks/functions.sql"),
            wrap_json=False,
            completion=self.sql_completed,
        )

        # Combine tables
        if self.args.combine_tables:
            await self.combine_tables(table_csv_files)
        return dump_files, table_csv_files

    async def combine_tables(self, table_csv_files):

        combine_tasks = []
        for table, csv_files in table_csv_files.items():
            file_tables = [f.stem for f in csv_files]
            if len(file_tables) < 1:
                continue
            if table in file_tables:
                logger.warning(
                    f"Cannot combine tables {file_tables} into {table} because they have the same name"
                )
                continue
            logger.info(f"Combining tables {file_tables} into {table}")
            table_schema_drop = f"DROP TABLE IF EXISTS import.{table} CASCADE;"
            table_schema_copy = f"CREATE TABLE import.{table} (LIKE import.{file_tables[0]} INCLUDING ALL);"
            subquery = str(" UNION ALL ").join(
                [f"SELECT * FROM import.{t}" for t in file_tables]
            )  # WHERE NOT ((strip(ID) = '') IS NOT FALSE)
            command = (
                f"INSERT INTO import.{table} SELECT * FROM ({subquery}) AS combined"
            )
            query = table_schema_drop + table_schema_copy + command
            logger.debug(query)
            combine_tasks.append(
                asyncio.create_task(
                    exec.exec_sql(
                        self.sql_db_options,
                        command=query,
                        wrap_json=False,
                        completion=self.sql_completed,
                    )
                )
            )  # Might throw column "id" does not exist
        await asyncio.gather(*combine_tasks)

    async def post_load_check(self, table_csv_files, csv_entries):
        logger.info("Running post load check")
        try:
            database_rows = {
                table: await exec.exec_sql(
                    self.sql_db_options,
                    command=f"SELECT count(*) FROM public.{table}",
                    sync=True,
                    completion=self.sql_completed,
                )
                for table in table_csv_files.keys()
            }

            table_header = [
                "table",
                "csv files",
                "total rows (csv files)",
                "total rows (database)",
                "difference",
            ]
            check_result = PrettyTable(table_header)
            for header in table_header:
                check_result.align[header] = "l"

            delta = 0
            for table, query_result in database_rows.items():
                try:
                    database_count = json.loads(query_result[0])[0].get("count", 0)
                except (json.decoder.JSONDecodeError, KeyError, TypeError) as e:
                    logger.error(e)
                    logger.error(query_result[1])
                    database_count = 0

                csv_files = table_csv_files.get(table, [])
                csv_file_entries = sum(
                    [csv_entries.get(str(csv_file), 0) for csv_file in csv_files]
                )
                difference = abs(csv_file_entries - database_count)
                delta += difference
                check_result.add_row(
                    [
                        table,
                        "omitted"
                        if len(csv_files) > 5
                        else [Path(f).stem for f in csv_files],
                        csv_file_entries,
                        database_count,
                        difference,
                    ]
                )
            logger.info("\n" + str(check_result))
            if delta > 100:
                logger.fatal(f"{delta} entries were not loaded into the database!")

        except Exception as e:
            logger.error(e)
            logger.error("Failed to get database entries from database")

    async def load(self, data_dirs):
        try:
            self.reset()

            # Step 0: Run Pre load script
            for pre_load_source in self.args.pre_load:
                pre_load_scripts = utils.files_in(pre_load_source, of_type="sql")
                [
                    logger.info(f"Executing pre load routine: {pre_load_script}")
                    for pre_load_script in pre_load_scripts
                ]
                pre_load_tasks = [
                    asyncio.create_task(
                        exec.exec_sql(
                            self.sql_db_options,
                            script=pre_load_script,
                            completion=self.sql_completed,
                        )
                    )
                    for pre_load_script in pre_load_scripts
                ]
                await asyncio.gather(*pre_load_tasks)

            # Step 1: Extract zipped files
            await self.step1_unzip(data_dirs)

            # Step 2: Import csv files into database
            dump_files, table_csv_files = await self.step2_import(data_dirs)

            # Step 3: Run post load script
            post_load_tasks = list()
            for post_load_source in self.args.post_load:
                post_load_scripts = list(
                    utils.files_in(post_load_source, of_type="sql")
                )
                [
                    logger.info(f"Executing post load routine: {post_load_script}")
                    for post_load_script in post_load_scripts
                ]
                post_load_tasks = [
                    asyncio.create_task(
                        exec.exec_sql(
                            self.sql_db_options,
                            script=post_load_script,
                            completion=self.sql_completed,
                        )
                    )
                    for post_load_script in post_load_scripts
                ]

            # Step 4: Count csv file rows
            logger.info("Counting csv file rows")
            csv_entries_task = asyncio.create_task(
                csvcount.count_csv_entries(dump_files)
            )
            post_load_tasks.append(csv_entries_task)
            await asyncio.gather(*post_load_tasks)
            csv_entries = csv_entries_task.result()

            # Step 5: Post load check
            if not self.args.disable_check:
                await self.post_load_check(table_csv_files, csv_entries)

            logger.info("Completed.")

        except asyncio.CancelledError:
            pass

    @staticmethod
    def _log_process_result(
        process,
        stderr,
        success_message=None,
        success_with_stderr_message=None,
        stderr_error_message=None,
        other_error_message=None,
    ):
        if process.returncode == 0:
            if stderr and success_with_stderr_message:
                logger.warning(success_with_stderr_message)
            logger.info(success_message)
        elif stderr:
            if stderr_error_message:
                logger.error(stderr_error_message)
        else:
            if other_error_message:
                logger.error(other_error_message)

    def log_process_result(self, task, cmd, process, stderr):
        self._log_process_result(
            process,
            stderr,
            success_message=f'Task "{task}" of {cmd} finished successfully',
            success_with_stderr_message=f'Task "{task}" of {cmd} wrote to stderr: \n"{stderr.decode() if stderr else ""}"',
            stderr_error_message=f'Task "{task}" of {cmd} errored with stderr output: \n"{stderr.decode() if stderr else ""}"',
            other_error_message=f'Task "{task}" of {cmd} errored without writing to stderr',
        )

    async def zip_completed(self, process, cmd, stderr):
        self.zip_done += 1
        src = cmd[1]
        self.log_process_result(task="Unzip", cmd=src, process=process, stderr=stderr)

    async def import_completed(self, process, cmd, stderr=None, stdout=None):
        src = cmd[-1]
        if src not in self.load_done.keys():
            self.load_done[src] = dict()
        self.load_done[src].update(percent=1.0)
        self.log_process_result(task="Import", cmd=src, process=process, stderr=stderr)

    async def sql_completed(self, process, cmd, stderr=None, stdout=None):
        script = cmd[-1]
        self.log_process_result(
            task="Execute SQL", cmd=script, process=process, stderr=stderr
        )

    async def import_received_output(self, _process, cmd):
        try:
            src = cmd[-1]
            progress_update = await asyncio.wait_for(
                _process.stdout.read(1024), timeout=5.0
            )
            progress_update = progress_update.decode("utf-8")
            logger.debug(str(progress_update))
            update = dict()
            try:
                percent = re.search(r"(\d?\d?\d.\d\d)%", progress_update).group(1)
                update.update(percent=min(float(percent) / 100.0, 1.0))
            except (AttributeError, ValueError, TypeError) as e:
                logger.debug(e)

            try:
                sizes = re.search(
                    r"(\d?\d?\d.\d\d)\sGiB\s/\s(\d?\d?\d.\d\d)\sGiB", progress_update
                )
                size_done, size_total = float(sizes.group(1)), float(sizes.group(2))
                update.update(size_done=size_done, size_total=size_total)
            except (AttributeError, ValueError, TypeError) as e:
                logger.debug(e)

            if src not in self.load_done.keys():
                self.load_done[src] = dict()
            self.load_done[src].update(update)
            logger.debug(self.load_done[src])
            return 10
        except asyncio.TimeoutError:
            logger.warning("Could not parse progress update (Timeout)")
        except Exception as e:
            logger.error("Could not parse progress update")
            logger.error(e)
            raise

    @staticmethod
    async def sql_received_output(_process, _cmd):
        try:
            progress_update = await asyncio.wait_for(
                _process.stdout.readline(), timeout=10.0
            )
            logger.info(progress_update.decode("utf-8"))
        except asyncio.TimeoutError:
            logger.debug("Did not receive output from sql script (Timeout)")
        except Exception as e:
            logger.error("Could not parse progress update")
            logger.error(e)
            raise

    async def unzip(self, files):
        self.zip_total = self.load_total = len(files)
        if len(files) < 1:
            logger.info("No files to unzip")
            return
        [logger.info(f"Unzipping {str(src.absolute())}") for src, dest in files]
        await exec.run_simultaneously(
            [
                ("unzip", ["-o", str(src.absolute()), "-d", str(dest.absolute())])
                for src, dest in files
            ]
        )
        task = asyncio.create_task(
            self.check_progress(completion_handler=self.zip_completed)
        )
        await asyncio.wait({task})

    async def import_data(self, table_csv_files, parallel=True):
        self.load_total = sum(
            [len(csv_files) for csv_files in table_csv_files.values()]
        )

        for table, csv_files in table_csv_files.items():
            [
                logger.info(f"Importing {csv_file} into {csv_file.stem}")
                for csv_file in csv_files
            ]

        if parallel:
            await exec.run_simultaneously(
                itertools.chain.from_iterable(
                    [
                        (
                            "pgfutter",
                            utils.to_cli_options(self.db_options)
                            + ["-table", csv_file.stem, "csv", str(csv_file)],
                        )
                        for csv_file in csv_files
                    ]
                    for table, csv_files in table_csv_files.items()
                ),
                queue=self.queue,
            )
            task = asyncio.create_task(
                self.check_progress(
                    output_handler=self.import_received_output,
                    completion_handler=self.import_completed,
                )
            )
            await asyncio.wait({task})
        else:
            for table, csv_files in table_csv_files.items():
                for csv_file in csv_files:

                    async def output_handler(*args, **kwargs):
                        await self.import_received_output(*args, **kwargs)
                        await self.update_progress()

                    task = asyncio.create_task(
                        exec.run(
                            "pgfutter",
                            utils.to_cli_options(self.db_options)
                            + ["-table", csv_file.stem, "csv", str(csv_file)],
                            output=output_handler,
                            completion=self.import_completed,
                            queue=self.queue,
                        )
                    )
                    await asyncio.wait({task})


async def shutdown(exit_signal, event_loop):
    logger.error(f"Received exit signal {exit_signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    Loader.executor.shutdown(wait=True)
    await asyncio.gather(*tasks, return_exceptions=True)
    event_loop.stop()


async def _main():
    args, unknown = cli.parse()
    if len(unknown) > 0:
        [logger.warning("Unknown argument %s" % arg) for arg in unknown]

    # Flatten hook script inputs
    args.sources = [item for sublist in args.sources or list() for item in sublist]
    args.post_load = [item for sublist in args.post_load or list() for item in sublist]
    args.pre_load = [item for sublist in args.pre_load or list() for item in sublist]

    # Set log level
    log_level = getattr(
        logging, "INFO" if not args.log_level else args.log_level.upper()
    )
    logging.basicConfig(level=log_level)
    if args.all:
        logger.warning(
            "Will unzip and import everything again. This might take a while..."
        )

    if len(args.sources) < 1:
        logger.fatal("No input files")
        return

    await Loader(args).load([Path(source) for source in args.sources])


def main():
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(s, loop))
        )

    try:
        loop.run_until_complete(_main())
    finally:
        loop.close()
