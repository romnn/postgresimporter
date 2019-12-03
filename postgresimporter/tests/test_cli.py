import os
import pathlib
from contextlib import contextmanager
from unittest import mock

from . import common


class CLITest(common.BaseTest):
    @contextmanager
    def assertLoadsFiles(self, files, from_files=None):
        with self.lock_create_subprocess() as mocked_subprocess_calls:
            yield pathlib.Path(os.path.commonprefix(from_files or []))

            expected_calls = [
                (
                    "pgfutter",
                    "-table",
                    table,
                    "csv",
                    *csv_files,
                    self.AnyArg(str),
                    self.AnyArg(str),
                )
                for table, csv_files in files.items()
            ]
            mocked_subprocess_calls.call_args_list = expected_calls

    def test_ignores_unzipping(self):
        """Test if --disable-unzip skips unzipping source archives

        :return:
        """
        files = [pathlib.Path("/test/a.zip")]
        with self.locked_harness(files) as mocks:
            mocks.mocked_unzip.return_value = common.noop
            self.load(mocks.paths, disable_unzip=False, all=False)
            mocks.mocked_unzip.assert_called_with(
                self.AnyArg(str),
                [(pathlib.Path("/test/a.zip"), pathlib.Path("/test/a"))],
            )

        with self.locked_harness(files) as mocks:
            mocks.mocked_import.return_value = common.noop
            self.load(mocks.paths, disable_unzip=True, all=False)
            mocks.mocked_import.assert_not_called()

    def test_ignores_loading(self):
        """Test if --disable-import skips importing into database

        :return:
        """
        files = ["/test/a.csv", "/test/b.csv"]
        with self.locked_harness(files) as mocks:
            mocks.mocked_import.return_value = common.noop
            self.load(mocks.paths, disable_import=False, all=False)
            mocks.mocked_import.assert_called_with(
                self.AnyArg(str),
                {
                    "a": [pathlib.Path("/test/a.csv")],
                    "b": [pathlib.Path("/test/b.csv")],
                },
            )

        with self.locked_harness(files) as mocks:
            mocks.mocked_import.return_value = common.noop
            self.load(mocks.paths, disable_import=True, all=False)
            mocks.mocked_import.assert_not_called()

    def test_executes_pre_load(self):
        """Test if --pre-load cli option will be executed

        :return:
        """
        files = ["/pl.sql"]
        with self.locked_harness(files) as mocks:
            mocks.mocked_exec_sql.return_value = common.noop
            self.load(mocks.paths, pre_load=[pathlib.Path("/pl.sql")])
            self.assertIn(
                mock.call(
                    self.AnyArg(dict),
                    completion=self.AnyArg(str),
                    script=pathlib.Path("/pl.sql"),
                ),
                mocks.mocked_exec_sql.call_args_list,
            )

        with self.locked_harness(files) as mocks:
            mocks.mocked_exec_sql.return_value = common.noop
            self.load(mocks.paths)
            self.assertEqual(
                mocks.mocked_exec_sql.call_args_list,
                [
                    mock.call(
                        self.AnyArg(dict),
                        completion=self.AnyArg(str),
                        script=pathlib.Path("hooks/functions.sql"),
                        wrap_json=False,
                    )
                ],
            )

    def test_executes_post_load(self):
        """Test if --post-load cli option will be executed

        :return:
        """
        files = ["/pl.sql"]
        with self.locked_harness(files) as mocks:
            mocks.mocked_exec_sql.return_value = common.noop
            self.load(mocks.paths, post_load=[pathlib.Path("/pl.sql")])
            self.assertIn(
                mock.call(
                    self.AnyArg(dict),
                    completion=self.AnyArg(str),
                    script=pathlib.Path("/pl.sql"),
                ),
                mocks.mocked_exec_sql.call_args_list,
            )

        with self.locked_harness(files) as mocks:
            mocks.mocked_exec_sql.return_value = common.noop
            self.load(mocks.paths)
            self.assertEqual(
                mocks.mocked_exec_sql.call_args_list,
                [
                    mock.call(
                        self.AnyArg(dict),
                        completion=self.AnyArg(str),
                        script=pathlib.Path("hooks/functions.sql"),
                        wrap_json=False,
                    )
                ],
            )

    def test_recreate_all(self):
        """Test if --all cli option unzips and imports all source files

        :return:
        """
        mock_files = ["/a.zip", "/a/test.csv", "/b.zip"]
        with self.locked_harness(mock_files) as mocks:
            self.load(mocks.paths, all=True)

            mocks.mocked_unzip.assert_called_with(
                self.AnyArg(str),
                sorted(
                    [
                        (pathlib.Path("/a.zip"), pathlib.Path("/a")),
                        (pathlib.Path("/b.zip"), pathlib.Path("/b")),
                    ]
                ),
            )
            mocks.mocked_import.assert_called_with(
                self.AnyArg(str), {"test": [pathlib.Path("/a/test.csv")]}
            )
