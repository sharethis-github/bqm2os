import unittest
from unittest import TestCase
from unittest.mock import Mock

import mock
from google.api_core.page_iterator import Iterator
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import QueryJob, SourceFormat, \
    WriteDisposition
from google.cloud.bigquery.table import Table

import resource
from resource import strictSubstring, Resource, \
    BqDatasetBackedResource, BqViewBackedTableResource, \
    BqQueryBasedResource, BqJobs, BqDataLoadTableResource, \
    processLoadTableOptions


class Test(unittest.TestCase):
    def test_getFiltered(self):
        self.assertTrue(resource.getFiltered(".") == ".")
        self.assertTrue(resource.getFiltered("@") == " ")

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
    def test_ComplicatedlegacyBqQueryDependsOnFunc(self, mock_Client:
    Client,
                                        mock_Table: Table,
                                        mock_Table2: Table):
        mock_Table.project = "yourproject:qualifier"
        mock_Table.name = "url_taxonomy_assignment_ranked_url_title_tokens_kw_20170601"
        mock_Table.dataset_name = "test"
        query = """#standardSQL

SELECT *
FROM (
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_0_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_1_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_2_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_3_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_4_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_5_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_6_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_7_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_8_url_title_tokens_kw_20170601`)
union all
(select * from `yourproject:qualifier.test.url_taxonomy_assignment_9_url_title_tokens_kw_20170601`)
)
"""

        left = BqQueryBasedResource(query, mock_Table, 0, mock_Client)

        mock_Table2.name = "url_taxonomy_assignment_8_url_title_tokens_kw_20170601"
        mock_Table2.dataset_name = "test"
        query2 = """select
    *,
    row_number() over (partition by id, description order by overlap desc) id_to_urls_rank
from (
select
  group_concat(unique(kw)) matching_kw,
  id,
  description,
  url,
  sum(float(tscore) * float(fscore)) overlap
from (
select id, description, url, kw, fscore, score tscore from [yourproject:qualifier:test.url_title_tokens_kw_20170601]
join each
(select id, description, feature, score fscore from
[yourproject:qualifier:test.kw_features_ranked]
where abs(hash(id)) % 10 == 8 ) fkw
on kw = fkw.feature
group each by id, description, url, kw, tscore, fscore
)
group each by id, description, url
)
"""

        right = BqQueryBasedResource(query2, mock_Table2, 0, mock_Client)
        self.assertTrue(left.dependsOn(right))
        self.assertFalse(right.dependsOn(left))

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.table.Table')
    def test_legacyBqQueryDependsOnFunc(self, mock_Client: Client,
                                        mock_Table: Table,
                                        mock_Table2: Table):
        mock_Table.name = "v1_"
        mock_Table.dataset_name = "dset"
        query = "... yourproject:qualifier:mergelog.v1_], " \
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

    @mock.patch('google.cloud.bigquery.table.Table')
    def test_buildDataSetKey_(self, table):
        table.project = 'p'
        table.dataset_id = 'd'
        actual = resource._buildDataSetKey_(table)
        expected = 'p:d'
        self.assertEqual(actual, expected)

    @mock.patch('google.cloud.bigquery.table.Table')
    def test_buildTableKey_(self, table: Table):
        table.project = 'p'
        table.dataset_id = 'd'
        table.friendly_name = 't'
        actual = resource._buildDataSetTableKey_(table)
        expected = 'p:d:t'
        self.assertEqual(actual, expected)

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.iterator.Iterator')
    @mock.patch('google.cloud.bigquery.job.QueryJob')
    @mock.patch('google.cloud.bigquery.table.Table')
    def testBqJobsLoadTableJobs(self, client: Client, it: Iterator,
                                job: QueryJob, table: Table):
        client.list_jobs.return_value = it
        it.next_page_token = None

        job.destination = table
        table.dataset_id = "d"
        table.friendly_name = "t"
        table.project = "p"

        jobs = BqJobs(client, {})
        client.list_jobs.return_value = it
        jobs.loadTableJobs()
        client.list_jobs.assert_has_calls([
            mock.call(max_results=1000, state_filter='pending'),
            mock.call(max_results=1000, state_filter='running')], any_order=True)

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.iterator.Iterator')
    @mock.patch('google.cloud.bigquery.job.QueryJob')
    @mock.patch('google.cloud.bigquery.table.Table')
    def testBqJobsLoadTableJobsRuning(self, client: Client, it: Iterator,
                                      job: QueryJob, table: Table):

        job.destination = table
        table.dataset_id = "d"
        table.friendly_name = "t"
        table.project = "p"

        jobs = BqJobs(client, {})
        it.page_number = 0
        it.next_page_token = False
        client.list_jobs.return_value = it

        it.__iter__ = Mock(return_value=iter([job]))

        jobs.__loadTableJobs__('running')
        self.assertEquals(jobs.tableToJobMap['p:d:t'], job)


    def testDetectSourceFormatForJson(self):
        self.assertEquals(
            SourceFormat.NEWLINE_DELIMITED_JSON,
            BqDataLoadTableResource.detectSourceFormat("[]"))

    def testDetectSourceFormatForCsv(self):
        self.assertEquals(
            SourceFormat.CSV,
            BqDataLoadTableResource.detectSourceFormat(
            "a"))

    @mock.patch('google.cloud.bigquery.job.LoadTableFromStorageJob')
    def test_HandleLoadTableOptionSourceFormat(self, sj):
        options = {
            "source_format": "NEWLINE_DELIMITED_JSON"
        }

        processLoadTableOptions(options, sj)
        self.assertEquals(sj.write_disposition, SourceFormat.NEWLINE_DELIMITED_JSON)

    @mock.patch('google.cloud.bigquery.job.LoadTableFromStorageJob')
    def test_HandleLoadTableOptionWriteDisposition(self, sj):
        options = {"write_disposition": "WRITE_TRUNCATE"}

        processLoadTableOptions(options, sj)
        self.assertEquals(sj.write_disposition,
                          WriteDisposition.WRITE_TRUNCATE)

    @mock.patch('google.cloud.bigquery.job.LoadTableFromStorageJob')
    def test_HandleLoadTableOptionInvalidDispositionOrFormat(self, sj):
        options = {"write_disposition": "invalid"}

        try:
            processLoadTableOptions(options, sj)
            self.fail("unknown value")
        except KeyError:
            pass

        options = {"source_format": "invalid"}
        try:
            processLoadTableOptions(options, sj)
            self.fail("unknown value")
        except KeyError:
            pass


