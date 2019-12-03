import unittest
import unittest.util

import blessings
import pygments
import pygments.formatters
from pygments.lexers import Python3TracebackLexer as Lexer


class ColoredTestResult(unittest.result.TestResult):
    """
    A tes result implementation based on
    https://github.com/meshy/colour-runner/blob/master/colour_runner/result.py
    """

    formatter = pygments.formatters.Terminal256Formatter()
    lexer = Lexer()
    separator1 = "=" * 70
    separator2 = "-" * 70
    indent = " " * 4

    _terminal = blessings.Terminal()
    colours = {
        None: str,
        "error": _terminal.bold_red,
        "expected": _terminal.blue,
        "fail": _terminal.bold_yellow,
        "skip": str,
        "success": _terminal.green,
        "title": _terminal.blue,
        "unexpected": _terminal.bold_red,
    }

    _test_class = None

    def __init__(self, stream, descriptions, verbosity, no_colour=False):
        super().__init__(stream, descriptions)
        self.stream = stream
        self.showAll = verbosity > 1
        self.dots = verbosity == 1
        self.descriptions = descriptions
        self.no_colour = no_colour

    def get_short_description(self, test):
        doc_first_line = test.shortDescription()
        if self.descriptions and doc_first_line:
            return self.indent + doc_first_line
        return self.indent + test._testMethodName

    def get_long_description(self, test):
        doc_first_line = test.shortDescription()
        if self.descriptions and doc_first_line:
            return "\n".join((str(test), doc_first_line))
        return str(test)

    def get_class_description(self, test):
        test_class = test.__class_under_test__
        doc = test_class.__doc__
        if self.descriptions and doc:
            return doc.strip().split("\n")[0].strip()
        return unittest.util.strclass(test_class)

    def startTest(self, test):
        super().startTest(test)
        if self.showAll:
            if self._test_class != test.__class__:
                self._test_class = test.__class__
                title = self.get_class_description(test)
                self.stream.writeln(self.colours["title"](title))
            self.stream.write(self.get_short_description(test))
            self.stream.write(" ... ")
            self.stream.flush()

    def print_result(self, short, extended, colour_key=None):
        if self.no_colour:
            colour = self.colours[None]
        else:
            colour = self.colours[colour_key]
        if self.showAll:
            self.stream.writeln(colour(extended))
        elif self.dots:
            self.stream.write(colour(short))
            self.stream.flush()

    def addSuccess(self, test):
        super().addSuccess(test)
        self.print_result(".", "ok", "success")

    def addError(self, test, err):
        super().addError(test, err)
        self.print_result("E", "ERROR", "error")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.print_result("F", "FAIL", "fail")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.print_result("s", "skipped {0!r}".format(reason), "skip")

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self.print_result("x", "expected failure", "expected")

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.print_result("u", "unexpected success", "unexpected")

    def printErrors(self):
        if self.dots or self.showAll:
            self.stream.writeln()
        self.printErrorList("ERROR", self.errors)
        self.printErrorList("FAIL", self.failures)

    def printErrorList(self, flavour, errors):
        if self.no_colour:
            colour = self.colours[None]
        else:
            colour = self.colours[flavour.lower()]

        for test, err in errors:
            self.stream.writeln(self.separator1)
            title = "%s: %s" % (flavour, self.get_long_description(test))
            self.stream.writeln(colour(title))
            self.stream.writeln(self.separator2)
            if self.no_colour:
                self.stream.writeln(err)
            else:
                self.stream.writeln(pygments.highlight(err, self.lexer, self.formatter))


class ColoredTestRunner(unittest.runner.TextTestRunner):
    """A test runner that uses colour in its output"""

    resultclass = ColoredTestResult

    def __init__(self, *args, **kwargs):
        self.no_colour = kwargs.pop("no_colour", False)
        super().__init__(*args, **kwargs)

    def _makeResult(self):
        return self.resultclass(
            self.stream, self.descriptions, self.verbosity, self.no_colour
        )
