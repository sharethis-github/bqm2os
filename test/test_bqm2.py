import unittest

from datetime import datetime
from google.cloud.bigquery.schema import SchemaField
import mock
from mock.mock import MagicMock

from bqm2 import DelegatingFileSuffixLoader, FileLoader, \
    parseDatasetTable, \
    parseDataset, BqDataFileLoader, BqQueryTemplatingFileLoader


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

    @mock.patch('google.cloud.bigquery.Client')
    def testParseDataSetTable(self, mock_client: MagicMock):
        parseDatasetTable("a/b/dataset.table.suffix", "default",
                              mock_client)
        mock_client.dataset.assert_called_with('dataset')
        mock_client.dataset().table.assert_called_with('table')

    @mock.patch('google.cloud.bigquery.Client')
    def testParseDataSetTableWithoutDataset(self, mock_client: MagicMock):
        parseDatasetTable("a/b/table.suffix", "dataset", mock_client)
        mock_client.dataset.assert_called_with('dataset')
        mock_client.dataset().table.assert_called_with('table')

    @mock.patch('google.cloud.bigquery.Client')
    def testParseDataSetTableWithoutDefaultDataset(self, mock_client):
        try:
            parseDatasetTable("a/b/table.suffix", None, mock_client)
            self.assertTrue(False)
        except:
            pass

    def testParseDataSet(self):
        self.assertEquals(parseDataset("a/b/dataset.suffix"), "dataset")

    def testParseSchemaString(self):
        expected = [SchemaField("a", "int"), SchemaField("b", "string")]
        result = BqDataFileLoader("dummy").loadSchemaFromString("a:int,"
                                                           "b:string")
        self.assertEquals(expected, result)

    def testExplodeTemplateVarsArray(self):
        from datetime import datetime, timedelta

        n = datetime.today()
        expectedDt = [dt.strftime("%Y%m%d") for dt in [n, n + timedelta(
                days=-1)]]
        template = {"folder": "afolder",
                    "foo": "bar_{folder}_{filename}",
                    "yyyymmdd": [-1, 0]}
        result = BqQueryTemplatingFileLoader\
                .explodeTemplateVarsArray([template],
                'afolder', 'afile', 'adataset')
        expected = [{'filename': 'afile', 'folder': 'afolder', 'dataset':
                    'adataset', 'yyyymmdd': expectedDt[1], 'foo':
                    'bar_afolder_afile', "table": "afile"},
                    {'filename': 'afile', 'folder': 'afolder', 'dataset':
                    'adataset', 'yyyymmdd': expectedDt[0], 'foo':
                        'bar_afolder_afile', "table": "afile"}]
        self.assertEqual(result, expected)



if __name__ == '__main__':
    unittest.main()
