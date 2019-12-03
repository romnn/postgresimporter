import asyncio
import functools
import logging
import os
import pathlib
import typing
import unittest
import unittest.mock
from argparse import Namespace
from contextlib import contextmanager

# pyfakefs must be imported at last
from pyfakefs import fake_filesystem_unittest

from postgresimporter.main import Loader

logging.getLogger("loader").setLevel(logging.FATAL)

noop = asyncio.coroutine(lambda: None)()


class Noop:
    def __await__(self) -> typing.Generator[None, None, None]:
        yield


def make_async(func):
    async def _func(*_args, **_kwargs):
        func(*_args, **_kwargs)

    return _func


class AsyncMock(unittest.mock.Mock):
    def __call__(self, *args, **kwargs):
        async def coroutine():
            return super(self).__call__(*args, **kwargs)

        return coroutine()

    def __await__(self):
        return self().__await__()


def run_sync(
    target: typing.Callable[..., typing.Any],
    *args,
    timeout: int = 10,
    new_loop: bool = False,
    **keywords,
) -> typing.Any:
    """Run async tasks synchronously with a timeout

    :param target:
    :param args:
    :param timeout:
    :param new_loop:
    :param keywords:
    :return:
    """
    loop = asyncio.new_event_loop() if new_loop else asyncio.get_event_loop()
    if new_loop:
        asyncio.set_event_loop(loop)

    async def wait(coroutine):
        try:
            return await asyncio.wait_for(coroutine(), timeout=timeout)
        except asyncio.TimeoutError:
            print("Timeout")
            return None

    try:
        result = loop.run_until_complete(
            wait(functools.partial(target, *args, **keywords))
        )
    finally:
        if new_loop:
            loop.close()
    return result


class Mocks:
    def __init__(
        self,
        paths,
        mocked_unzip,
        mocked_import,
        mocked_csv_counter,
        mocked_exec_sql,
        mocked_subprocess,
    ):
        self.paths = paths
        self.mocked_unzip = mocked_unzip
        self.mocked_import = mocked_import
        self.mocked_csv_counter = mocked_csv_counter
        self.mocked_exec_sql = mocked_exec_sql
        self.mocked_subprocess = mocked_subprocess


class BaseTest(unittest.TestCase):
    @classmethod
    def AnyArg(cls, _cls):
        class _AnyArg:
            def __eq__(self, other):
                return True

        return _AnyArg()

    @property
    def __class_under_test__(self):
        return self.__class__

    @contextmanager
    def lock_create_subprocess(self):
        with unittest.mock.patch(
            "asyncio.create_subprocess_exec", autospec=True
        ) as mocked_subprocess:
            mock_process = unittest.mock.Mock()

            mock_communicate, mock_read = asyncio.Future(), asyncio.Future()
            mock_communicate.set_result((None, None))
            mock_read.set_result(None)
            mock_process.communicate.return_value = mock_communicate
            mock_process.stdout.read.return_value = mock_read

            mock_subprocess_result = asyncio.Future()
            mock_subprocess_result.set_result(mock_process)

            mocked_subprocess.return_value = mock_subprocess_result
            yield mocked_subprocess

    @contextmanager
    def locked_harness(self, mock_files=None):
        with self.create_mock_files(mock_files) as paths:
            with self.lock_create_subprocess() as mocked_subprocess:
                with unittest.mock.patch(
                    "postgresimporter.utils.packaged", autospec=True
                ) as mocked_package_file:
                    with unittest.mock.patch(
                        "postgresimporter.csvcount._count_csv_entries", autospec=True
                    ) as mocked_csv_counter:
                        with unittest.mock.patch(
                            "postgresimporter.main.Loader.unzip", autospec=True
                        ) as mocked_unzip:
                            with unittest.mock.patch(
                                "postgresimporter.main.Loader.import_data",
                                autospec=True,
                            ) as mocked_import:
                                with unittest.mock.patch(
                                    "postgresimporter.exec.exec_sql", autospec=True
                                ) as mocked_exec_sql:
                                    mocked_package_file.side_effect = lambda module, file: pathlib.Path(
                                        file
                                    )

                                    mock_unzip = asyncio.Future()
                                    mock_unzip.set_result(None)
                                    mocked_unzip.return_value = mock_unzip

                                    mock_import = asyncio.Future()
                                    mock_import.set_result(None)
                                    mocked_import.return_value = mock_import

                                    mock_exec_sql = asyncio.Future()
                                    mock_exec_sql.set_result(None)
                                    mocked_exec_sql.return_value = mock_exec_sql

                                    mocked_csv_counter.return_value = make_async(
                                        lambda: {f: 10 for f in mock_files or []}
                                    )

                                    yield Mocks(
                                        paths,
                                        mocked_unzip,
                                        mocked_import,
                                        mocked_csv_counter,
                                        mocked_exec_sql,
                                        mocked_subprocess,
                                    )

    @contextmanager
    def create_mock_files(self, mock_files):
        with fake_filesystem_unittest.Patcher() as patcher:
            [patcher.fs.create_file(file) for file in mock_files]
            yield [pathlib.Path(os.path.commonprefix(mock_files or []))]

    @staticmethod
    def load(paths, **args):
        default_args = dict(
            pre_load=list(),
            post_load=list(),
            disable_unzip=True,
            disable_check=True,
            disable_import=True,
            combine_tables=False,
            all=False,
            db_name=None,
            db_host=None,
            db_port=None,
            db_user=None,
            db_password=None,
            exclude_regex=None,
        )
        run_sync(
            Loader(Namespace(**{**default_args, **args}), progress=False).load,
            paths,
            timeout=100,
        )
