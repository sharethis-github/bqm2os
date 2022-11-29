import unittest

from datetime import datetime, timedelta

from frozendict import frozendict

from tmplhelper import explodeTemplate, handleDateField, evalTmplRecurse


class Test(unittest.TestCase):

    def testEvalTmplRecurseCircular(self):
        input = {"a": '{b}', 'b': "{a}"}
        try:
            evalTmplRecurse(input)
            self.assertTrue(False, "We should have blown up")
        except Exception:
            pass

    def testEvalTmplRecurseSimple(self):
        input = {"a": '{b}', 'b': "c"}
        expected = {'a': 'c', 'b': 'c'}
        result = evalTmplRecurse(input)
        self.assertEqual(expected, result)

    def testEvalTmplRecurseWithUscoreSuffix(self):
        input = {'b_dash2uscore': "c-a"}
        expected = {'b_dash2uscore': 'c_a'}
        result = evalTmplRecurse(input)
        self.assertEqual(expected, result)

    def testEvalTmplRecurseRecursive(self):
        input = {"a": '{b}', 'b': "{c}", 'c': "d"}
        expected = {'a': 'd', 'b': 'd', 'c': 'd'}
        result = evalTmplRecurse(input)
        self.assertEqual(expected, result)

    def testEvalTmplRecurseCompoundValuesRecursive(self):
        input = {"a": '{c}_{e}', 'b': "{c}", 'c': "d", 'e': 'f'}
        expected = {'a': 'd_f', 'b': 'd', 'c': 'd', 'e': 'f'}
        result = evalTmplRecurse(input)
        self.assertEqual(expected, result)

    def testBuildTemplateFromTemplateVars(self):

        n = datetime.today()
        expectedDt = n + timedelta(days=-1)
        dt = expectedDt.strftime("%Y%m%d")
        yyyy = expectedDt.strftime("%Y")
        mm = expectedDt.strftime("%m")
        dd = expectedDt.strftime("%d")
        yy = expectedDt.strftime("%y")

        templateVars = {"filename": "fname",
                        "table": "{filename}_{keywords_table}",
                        "keywords_table": "url_kw_{yyyymmdd}",
                        "overlap_threshold": "0.2", "yyyymmdd": -1}

        expected = {'keywords_table': 'url_kw_' + dt, 'filename': 'fname',
                    'yyyymmdd': dt, 'table': 'fname_url_kw_' + dt,
                    'overlap_threshold': '0.2',
                    "yyyymmdd_dd": dd,
                    "yyyymmdd_mm": mm, "yyyymmdd_yy": yy,
                    "yyyymmdd_yyyy": yyyy}

        result = evalTmplRecurse(explodeTemplate(templateVars)[0])
        self.assertEqual(expected, result)

    def testBuildTemplateFromBadBug(self):
        n = datetime.today()
        expectedDt = n + timedelta(days=-1)
        templateVars = {
            "filename": "myfile",
            "table": "{filename}_{keywords_table}_{kw}_{yyyymmdd}_{modulo_val}",
            "keywords_table": "{kw_features_table}",
            "kw_features_table": ["kw_features_ranked",
                                  "kw_expansion_ranked"],
            "yyyymmdd": -1,
            "kw": ["url_kw", "url_title_tokens_kw", "url_url_tokens_kw"],
            "modulo_val": ["0", "1", "2", "3"],
            "modulo": "4"
        }

        result = explodeTemplate(templateVars)
        result = [evalTmplRecurse(x) for x in result]
        tables = set([x['table'] for x in result])
        expectedSet = ['myfile_kw_features_ranked_url_kw_{dt}_2',
                       'myfile_kw_expansion_ranked_url_title_tokens_kw_{dt}_1',
                       'myfile_kw_expansion_ranked_url_url_tokens_kw_{dt}_2',
                       'myfile_kw_expansion_ranked_url_title_tokens_kw_{dt}_3',
                       'myfile_kw_features_ranked_url_kw_{dt}_0',
                       'myfile_kw_features_ranked_url_title_tokens_kw_{dt}_3',
                       'myfile_kw_expansion_ranked_url_kw_{dt}_1',
                       'myfile_kw_features_ranked_url_url_tokens_kw_{dt}_2',
                       'myfile_kw_features_ranked_url_title_tokens_kw_{dt}_1',
                       'myfile_kw_features_ranked_url_url_tokens_kw_{dt}_0',
                       'myfile_kw_expansion_ranked_url_url_tokens_kw_{dt}_1',
                       'myfile_kw_features_ranked_url_kw_{dt}_1',
                       'myfile_kw_features_ranked_url_title_tokens_kw_{dt}_0',
                       'myfile_kw_features_ranked_url_url_tokens_kw_{dt}_3',
                       'myfile_kw_expansion_ranked_url_title_tokens_kw_{dt}_0',
                       'myfile_kw_expansion_ranked_url_url_tokens_kw_{dt}_3',
                       'myfile_kw_expansion_ranked_url_kw_{dt}_0',
                       'myfile_kw_features_ranked_url_kw_{dt}_3',
                       'myfile_kw_expansion_ranked_url_url_tokens_kw_{dt}_0',
                       'myfile_kw_expansion_ranked_url_title_tokens_kw_{dt}_2',
                       'myfile_kw_features_ranked_url_url_tokens_kw_{dt}_1',
                       'myfile_kw_features_ranked_url_title_tokens_kw_{dt}_2',
                       'myfile_kw_expansion_ranked_url_kw_{dt}_2',
                       'myfile_kw_expansion_ranked_url_kw_{dt}_3']
        expectedSet = set([x.format(dt=expectedDt.strftime("%Y%m%d")) for
                           x in expectedSet])
        self.assertEqual(tables, expectedSet)

    def testHandleDayDateFieldIntFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, -1, "yyyymmdd")
        expected = ['20051230']
        self.assertEqual(result, expected)

    def testHandleDayDateFieldIntArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, [-1, -3], "yyyymmdd")
        expected = sorted(['20051230', '20051229', '20051228'])
        self.assertEqual(result, expected)

    def testHandleHourDateFieldIntArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, [-1, -3], "yyyymmddhh")
        expected = sorted(['2005123023', '2005123022', '2005123021'])
        self.assertEqual(result, expected)    

    def testHandleMonthDateFieldIntArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, [-1, -3], "yyyymm")
        expected = sorted(['200511', '200510', '200509'])
        self.assertEqual(result, expected)   

    def testHandleYearDateFieldIntArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, [-1, -3], "yyyy")
        expected = sorted(['2004', '2003', '2002'])
        self.assertEqual(result, expected)   

    def testHandleDayDateFieldIntStringArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDateField(d, ["-1", -3], "yyyymmdd")
        expected = sorted(['20051230', '20051229', '20051228'])
        self.assertEqual(result, expected)

    def testExplodeTemplateSingleVar(self):
        templateVars = {"table": "{filename}_{keywords_table}",
                        "keywords_table": "url_kw",
                        "overlap_threshold": "0.2"}

        result = explodeTemplate(templateVars)
        self.assertEqual([templateVars], result)

    def testExplodeTemplateOneArray(self):
        templateVars = {"table": "{filename}_{keywords_table}",
                        "keywords_table": ["url_kw", "url_kw_title"],
                        "overlap_threshold": "0.2"}

        expected = [
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw",
             "overlap_threshold": "0.2"},
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw_title",
             "overlap_threshold": "0.2"}
        ]
        result = explodeTemplate(templateVars)
        self.assertEqual(expected, result)

    def testExplodeTemplateTwoArray(self):
        templateVars = {"table": "{filename}_{keywords_table}",
                        "keywords_table": ["url_kw", "url_kw_title"],
                        "overlap_threshold": ["0.2", "0.1"]}

        expected = [
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw",
             "overlap_threshold": "0.2"},
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw",
             "overlap_threshold": "0.1"},
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw_title",
             "overlap_threshold": "0.2"},
            {"table": "{filename}_{keywords_table}",
             "keywords_table": "url_kw_title",
             "overlap_threshold": "0.1"}
        ]
        expected = set([frozendict(x) for x in expected])

        result = explodeTemplate(
            templateVars)
        result = set(frozendict(x) for x in result)
        self.assertEqual(expected, result)


if __name__ == '__main__':
    unittest.main()
