import unittest

import mock
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.table import Table

from resource import strictSubstring, Resource, \
    BqDatasetBackedResource, BqViewBackedTableResource, \
    BqQueryBasedResource


class Test(unittest.TestCase):
    def test_strictSubstring(self):
        self.assertTrue(strictSubstring("A", "AA"))
        self.assertFalse(strictSubstring("A", "A"))
        self.assertTrue(strictSubstring("A", " Asxx "))

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.table.Table')
    @mock.patch('google.cloud.bigquery.Dataset')
    def test_realExampleOfSubstringingMisMatch(self, mock_Client: Client,
                                               mock_Table: Table,
                                               mock_Dataset: Dataset):
        input = "FROM taxonomy.url_kw_expansion_assignment_descendant """
        mock_Dataset.name = "taxonomy"
        mock_Table.name = "atable_on_something"
        mock_Table.dataset_name = "taxonomy"

        query = BqQueryBasedResource(input, mock_Table, 0, mock_Client)
        dataset = BqDatasetBackedResource(mock_Dataset, 0, mock_Client)

        self.assertFalse(dataset.dependsOn(query))
        self.assertTrue(query.dependsOn(dataset))

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.table.Table')
    def test_legacyBqQueryDependsOnFunc(self, mock_Client: Client,
                                        mock_Table: Table,
                                        mock_Table2: Table):
        mock_Table.name = "v1_"
        mock_Table.dataset_name = "dset"
        query = "... sharethis.com:quixotic-spot-526:mergelog.v1_], " \
                "DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'), ... "

        left = BqQueryBasedResource(query, mock_Table, 0, mock_Client)

        mock_Table2.name = "v1_"
        mock_Table2.dataset_name = "mergelog"
        query2 = "select 1 as one"

        right = BqQueryBasedResource(query2, mock_Table2, 0, mock_Client)
        self.assertTrue(left.dependsOn(right))
        self.assertFalse(right.dependsOn(left))

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.table.Table')
    @mock.patch('google.cloud.bigquery.Dataset')
    def test_DatasetDependency(self, mock_Client: Client,
                               mock_Table: Table, mock_Dataset: Dataset):

        mock_Dataset.name = "mergelog"
        mock_Table.name = "aview_on_something"
        mock_Table.dataset_name = "mergelog"

        dataset = BqDatasetBackedResource(mock_Dataset, 0, mock_Client)
        view = BqViewBackedTableResource("select * from mergelog.foobar",
                                  mock_Table, 0, mock_Client)

        self.assertTrue(view.dependsOn(dataset))
        self.assertFalse(dataset.dependsOn(view))
