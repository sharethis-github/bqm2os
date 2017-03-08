import unittest

import mock
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.table import Table

from rsrc.Rsrc import strictSubstring, legacyBqQueryDependsOn, Resource, \
    BqDatasetBackedResource, BqViewBackedTableResource


class Test(unittest.TestCase):
    def test_strictSubstring(self):
        self.assertTrue(strictSubstring("A", "AA"))
        self.assertFalse(strictSubstring("A", "A"))
        self.assertTrue(strictSubstring("A", " Asxx "))

    def test_realExampleOfSubstringingMisMatch(self):
        input = """select ml.estid estid, descendants.id segment,
                        descendants.id_desc description from
                (
                SELECT
                  estid,
                  url
                FROM
                  taxonomy.estid_url)  ml
                JOIN EACH (
                  SELECT
                    id,
                    url as aUrl
                  FROM
                    taxonomy.url_kw_expansion_assignment_descendant
                    ) s2url
                on s2url.aUrl = ml.url
                JOIN EACH (
                  select id, descendant_id, id_desc from [
                  taxonomy.categories_hierarchy_descendants]
                  ) descendants
                ON descendants.descendant_id = s2url.id
                group each by estid, segment, description
                """

        other = Resource()
        _self = Resource()
        def s():
            return "taxonomy.url_kw_expansion_assignment"
        _self.key = s

        def key():
            return "taxonomy.url_kw_expansion_assignment_descendant_estid"

        other.key = key
        self.assertFalse(legacyBqQueryDependsOn(_self, other, input))

    def test_legacyBqQueryDependsOnFunc(self):
        query = "... sharethis.com:quixotic-spot-526:mergelog.v1_], " \
                "DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'), ... "
        resource = Resource()
        _self = Resource()
        def s():
            return "xxxx"
        _self.key = s

        def key():
            return "mergelog.v1_"
        resource.key = key
        self.assertTrue(legacyBqQueryDependsOn(_self, resource, query))

    def test_legacyBqQueryDependsOnFuncForQueryString(self):
        _self = Resource()
        def s():
            return "xxxx"
        _self.key = s

        common = "atable_name_with_common_prefix"
        query = "... sharethis.com:quixotic-spot-526:mergelog" \
                "." + common + "_but_not_same], " \
                "DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'), ... "
        resource = Resource()
        def key():
            return "mergelog." + common
        resource.key = key
        self.assertFalse(legacyBqQueryDependsOn(_self, resource,
                                                query))

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
