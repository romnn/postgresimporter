import os
import pathlib
from contextlib import contextmanager
from unittest import mock

import common


class UnzipTest(common.BaseTest):
    @contextmanager
    def assertUnzipsFiles(self, files, from_files=None):
        with self.lock_create_subprocess() as mocked_subprocess_calls:
            yield [pathlib.Path(os.path.commonprefix(from_files or []))]
            for zip_file, extracted_file in files.items():
                self.assertIn(
                    mock.call(
                        "unzip",
                        "-o",
                        zip_file,
                        "-d",
                        extracted_file,
                        stderr=-1,
                        stdout=-1,
                    ),
                    mocked_subprocess_calls.call_args_list,
                )

    def test_finds_zipped_files(self):
        """Test if all zipped files in the source directory are found and unzipped

        :return:
        """
        base = "/sources"
        zips = {
            "/test/import/a.b.zip": "/test/import/a.b",
            "/test/import/x_y_z.zip": "/test/import/x_y_z",
        }
        zips = {base + k: base + v for k, v in zips.items()}
        mock_files = list(zips.keys()) + [
            base + "/test/import/mock_dir.ZIP",
            base + "/test/import/zip",
            base + "/test/import/a.b",
            base + "/test/import/mock_dir",
        ]
        with self.create_mock_files(mock_files):
            with self.assertUnzipsFiles(
                {base + "/test/import/x_y_z.zip": base + "/test/import/x_y_z"},
                from_files=mock_files,
            ) as paths:
                self.load(paths, disable_unzip=False)
