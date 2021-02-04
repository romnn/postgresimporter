import typing
import unittest

import test_runner


def test_cases(**_kwargs):

    import test_cli
    import test_load
    import test_unzip

    cases = list()
    cases += [
        test_cli.CLITest,
        test_load.LoadTest,
        test_unzip.UnzipTest,
    ]
    return cases


def test_suite(**kwargs) -> typing.Union[unittest.TestSuite, unittest.TestCase]:
    suite = unittest.TestSuite()
    suite.addTests(
        [
            unittest.defaultTestLoader.loadTestsFromTestCase(case)
            for case in test_cases(**kwargs)
        ]
    )
    return suite


if __name__ == "__main__":
    # Run the test suite
    result = test_runner.ColoredTestRunner(failfast=True, verbosity=2).run(test_suite())
    if result.wasSuccessful():
        exit(0)
    else:
        exit(1)
