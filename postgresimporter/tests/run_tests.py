import typing
import unittest

from postgresimporter.tests.test_runner import ColoredTestRunner


def test_cases(**_kwargs):

    import postgresimporter.tests.test_cli
    import postgresimporter.tests.test_load
    import postgresimporter.tests.test_unzip

    cases = list()
    cases += [
        postgresimporter.tests.test_cli.CLITest,
        postgresimporter.tests.test_load.LoadTest,
        postgresimporter.tests.test_unzip.UnzipTest,
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
    result = ColoredTestRunner(failfast=True, verbosity=2).run(test_suite())
    if result.wasSuccessful():
        exit(0)
    else:
        exit(1)
