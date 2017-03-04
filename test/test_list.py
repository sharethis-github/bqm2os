import unittest

from list import DelegatingFileSuffixLoader, FileLoader, parseDatasetTable, \
    parseDataset, applyTemplate


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

    def testParseDataSetTable(self):
        v = parseDatasetTable("a/b/dataset.table.suffix")
        self.assertEquals(v, ("dataset", "table"))

    def testParseDataSetTableWithoutDataset(self):
        v = parseDatasetTable("a/b/table.suffix", defaultDataset="dataset")
        self.assertEquals(v, ("dataset", "table"))

    def testParseDataSetTableWithoutDefaultDataset(self):
        try:
            parseDatasetTable("a/b/table.suffix")
            self.assertTrue(False)
        except:
            pass

    def testParseDataSet(self):
        self.assertEquals(parseDataset("a/b/dataset.suffix"), "dataset")

    def testApplyTemplate(self):
        s="a template"
        v=applyTemplate("a template", foo="bar")
        self.assertEquals(s, v)


if __name__ == '__main__':
    unittest.main()
