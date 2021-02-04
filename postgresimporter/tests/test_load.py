import itertools
import pathlib
from contextlib import contextmanager
from unittest import mock

import common

from postgresimporter import utils


class LoadTest(common.BaseTest):
    @contextmanager
    def assertLoadsFiles(self, files):
        with self.lock_create_subprocess() as mocked_subprocess:
            yield
            expected_calls = list(
                itertools.chain(
                    *[
                        [
                            mock.call(
                                "pgfutter",
                                "-table",
                                pathlib.Path(csv_file).stem,
                                "csv",
                                csv_file,
                                stderr=-1,
                                stdout=-1,
                            )
                            for csv_file in csv_files
                        ]
                        for table, csv_files in files.items()
                    ]
                )
            )
            for expected_call in expected_calls:
                self.assertIn(expected_call, mocked_subprocess.call_args_list)

    def test_chooses_correct_table(self):
        """Test if appropriate table names are chosen for csv input files

        :return:
        """
        # Expected input
        self.assertEqual(
            utils.table_name_for_path("/Train Data/2019 - 01/running_jan19.csv"),
            "running",
        )
        self.assertEqual(
            utils.table_name_for_path("/Train Data/2019 - 01/train_jan19.csv"), "train"
        )
        self.assertEqual(
            utils.table_name_for_path("/Train Data/2019 - 02/running_feb19.csv"),
            "running",
        )
        self.assertEqual(
            utils.table_name_for_path("/Train Data/2019 - 02/train_feb19.csv"), "train"
        )
        self.assertEqual(
            utils.table_name_for_path("/Train Data/running/timetable_feb19.csv"),
            "timetable",
        )

        # Unexpected input
        self.assertEqual(
            utils.table_name_for_path("/Train Data/running/example.test.csv"),
            "example",
        )
        self.assertEqual(
            utils.table_name_for_path("/Train Data/running/timetable_feb19.csv"),
            "timetable",
        )

    def test_finds_csv_files(self):
        """Test if all csv files in the source directory are found and imported

        :return:
        """
        base = "/sources"
        running_csv_files = [
            base + "/test/import/2019 - 01/running_jan19.csv",
            base + "/test/import/2019 - 02/running_feb19.csv",
        ]
        timetable_csv_files = [
            base + "/test/import/2019 - 01/timetables_jan19.csv",
            base + "/test/import/2019 - 02/timetables_feb19.csv",
        ]
        train_csv_files = [
            base + "/test/import/2019 - 01/train_jan19.csv",
            base + "/test/import/2019 - 02/train_feb19.csv",
        ]
        sample_csv_files = [
            base + "/test/import/2019 - 01/running_jan10_sample.csv",
            base + "/test/import/2019 - 02/timetables_jan19_sample.csv",
        ]
        csv_files = {
            running_csv_files[0]: "running",
            timetable_csv_files[0]: "timetable",
            train_csv_files[0]: "train",
            running_csv_files[1]: "running",
            timetable_csv_files[1]: "timetable",
            train_csv_files[1]: "train",
            # Sample files should be excluded
            sample_csv_files[0]: None,
            sample_csv_files[1]: None,
        }
        mock_files = list(csv_files.keys()) + [
            base + "/test/import/mock_dir.ZIP",
            base + "/test/import/zip",
            base + "/test/import/a.b",
            base + "/test/import/mock_dir",
        ]
        with self.create_mock_files(mock_files) as paths:
            with self.assertLoadsFiles(
                {
                    "running": running_csv_files,
                    "train": train_csv_files,
                    "timetable": timetable_csv_files,
                }
            ):
                self.load(paths, disable_import=False, exclude_regex="^.*sample.*$")
