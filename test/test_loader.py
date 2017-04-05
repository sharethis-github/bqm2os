import unittest

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
import mock
from mock.mock import MagicMock

from loader import DelegatingFileSuffixLoader, FileLoader, \
    parseDatasetTable, \
    parseDataset, BqDataFileLoader, BqQueryTemplatingFileLoader, TableType
from resource import BqJobs, BqViewBackedTableResource, \
    BqQueryBackedTableResource


class Test(unittest.TestCase):

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('resource.BqJobs')
    def test_ToggleToTableForTemplatingLoader(self, bqClient: Client,
                                           bqJobs: BqJobs):
        self.toggleToTableOrViewForTemplatingLoader(bqClient, bqJobs,
                                                    TableType.TABLE,
                                                    BqQueryBackedTableResource)

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('resource.BqJobs')
    def test_ToggleToViewForTemplatingLoader(self, bqClient: Client,
                                                  bqJobs: BqJobs):
        self.toggleToTableOrViewForTemplatingLoader(bqClient, bqJobs,
                                                    TableType.VIEW,
                                                    BqViewBackedTableResource)

    def toggleToTableOrViewForTemplatingLoader(self, bqClient: Client,
                                                    bqJobs: BqJobs,
                                                    tableType: TableType,
                                                    theType):

        bqClient.dataset('adataset').table('atable').name = 'atable'
        bqClient.dataset('adataset').table('atable').dataset_name = \
            'adataset'
        bqClient.dataset('adataset').table('atable').project = 'aproject'

        ldr = BqQueryTemplatingFileLoader(bqClient, bqJobs,
                                          tableType,
                                          defaultDataset="dataset")
        self.assertEqual(ldr.tableType, tableType)
        out = {}
        ldr.processTemplateVar({ "dataset": "dataset", "table": "foo"},
                               "select * from fiddle.sticks",
                               "afilepath", 0, out)
        key = "aproject:adataset:atable"
        self.assertTrue(key in out)
        self.assertTrue(isinstance(out[key], theType))


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

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('resource.BqJobs')
    def testProcessTemplateVarHappyPath(self, bqClient, bqJobs):

        bqClient.dataset('adataset').table('atable').name = 'atable'
        bqClient.dataset('adataset').table('atable').dataset_name = \
            'adataset'
        bqClient.dataset('adataset').table('atable').project = 'aproject'

        loader = BqQueryTemplatingFileLoader(bqClient, bqJobs,
                                             TableType.TABLE, 'default')
        templateVar = {
            'table': 'atable',
            'dataset': 'adataset',
            "foo": "bar"
        }
        output = {}
        loader.processTemplateVar(templateVar, "select * from {foo}",
                              "filepath", 0, output)
        self.assertTrue(len(output))
        self.assertTrue("aproject:adataset:atable" in output)
        arsrc = output["aproject:adataset:atable"]
        self.assertEqual(arsrc.query, "select * from bar")

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('resource.BqJobs')
    def testProcessTemplateVarMissingDataset(self, bqClient, bqJobs):

        # templateVars: dict, template: str,
        # filePath: str, mtime: int, out: dict

        loader = BqQueryTemplatingFileLoader(bqClient, bqJobs,
                                             TableType.TABLE,'default')
        templateVar = {
            'table': 'atable',
        }
        output = {}
        try:
            loader.processTemplateVar(templateVar, "select * from foo",
                                  "filepath", 0, output)
            self.fail("Should have thrown exception for missing dataset")
        except Exception:
            pass

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('resource.BqJobs')
    def testProcessTemplateVarMissingTable(self, bqClient, bqJobs):

        # templateVars: dict, template: str,
        # filePath: str, mtime: int, out: dict

        loader = BqQueryTemplatingFileLoader(bqClient, bqJobs,
                                             TableType.TABLE, 'default')
        templateVar = {
            'dataset': 'adataset',
        }
        output = {}
        try:
            loader.processTemplateVar(templateVar, "select * from foo",
                                  "filepath", 0, output)
            self.fail("Should have thrown exception for missing dataset")
        except Exception:
            pass

if __name__ == '__main__':
    unittest.main()
