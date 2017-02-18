import unittest

from list import DelegatingFileSuffixLoader, FileLoader


class Test(unittest.TestCase):
    def test_DelegatingFileLoader_EmptyLoader(self):
        try:
            DelegatingFileSuffixLoader()
            self.assertTrue(False, "Should have thrown exception on empty constructor")
        except:
            pass

    def test_DelegatingFileLoaderValidLoader(self):
        aLoader = FileLoader()
        DelegatingFileSuffixLoader(query=aLoader)

    def test_DelegatingFileLoaderInValidLoader(self):
        class Foo:
            pass

        aLoader = Foo()
        try:
            DelegatingFileSuffixLoader(query=aLoader)
            self.assertTrue(False, "Should have failed")
        except ValueError:
            pass

    def test_DelegatingFileLoaderParseInvalidSuffix(self):
        aLoader = FileLoader()
        try:
            DelegatingFileSuffixLoader(query=aLoader).load("nosuffixfile")
            self.assertTrue(False, "Should have failed with index error switched to value error")
        except ValueError:
            pass


    def test_DelegatingFileLoaderParseSuffix(self):
        aLoader = FileLoader()
        def f(file):
            return True
        aLoader.load = f
        self.assertTrue(DelegatingFileSuffixLoader(query=aLoader).load("nosuffixfile.query"))

if __name__ == '__main__':
    unittest.main()
