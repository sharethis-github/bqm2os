import unittest

from datetime import datetime, timedelta

from frozendict import frozendict

from tmplhelper import explodeTemplate, handleDayDateField, evalTmplRecurse, keysOfTemplate, computeImpliedRequiredVars, \
    explodeTemplateVarsArray


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

        templateVars = {"filename": "fname",
                        "table": "{filename}_{keywords_table}",
                        "keywords_table": "url_kw_{yyyymmdd}",
                        "overlap_threshold": "0.2", "yyyymmdd": -1}

        expected = {'keywords_table': 'url_kw_' + dt, 'filename': 'fname',
                    'yyyymmdd': dt, 'table': 'fname_url_kw_' + dt,
                    'overlap_threshold': '0.2'}
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
        result = handleDayDateField(d, -1)
        expected = ['20051230']
        self.assertEqual(result, expected)

    def testHandleDayDateFieldIntArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDayDateField(d, [-1, -3])
        expected = sorted(['20051230', '20051229', '20051228'])
        self.assertEqual(result, expected)

    def testHandleDayDateFieldIntStringArrayFormat(self):
        d = datetime.strptime('20051231', '%Y%m%d')
        result = handleDayDateField(d, ["-1",
                                        -3])
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


    def testKeysOfTemplateWithArray(self):
        self.assertEquals(set(['a','b','c','d']),
                          keysOfTemplate(["{a}_{b}", "{c}_{d}"]))

    def testKeysOfTemplate(self):
        self.assertEquals(set(['a','b']), keysOfTemplate("{a}_{b}"))

    def testKeysOfTemplateEmpty(self):
        self.assertEquals(set([]), keysOfTemplate("ab.foo"))

    def testComputeImpliedRequiredKeysSimple(self):
        requiredVars = set(['a'])
        templateVars = { "a": "{b}", "b": "c"}

        ret = computeImpliedRequiredVars(requiredVars, templateVars)
        self.assertEquals(set(['a', 'b']), ret)

    def testComputeImpliedRequiredKeysTwoLevels(self):
        requiredVars = set(['a'])
        templateVars = { "a": "{b}", "b": "{c}", "c": "d"}

        ret = computeImpliedRequiredVars(requiredVars, templateVars)
        self.assertEquals(set(['a', 'b', 'c']), ret)

    def testExplodeTemplateVarsArray(self):
        requiredVars = set(['a'])
        rawTemplates = [{'a': '{b}', 'c': 'd'}]
        defaultVars = {"b": "x"}
        result = explodeTemplateVarsArray(requiredVars, rawTemplates, defaultVars)

        expected = [{'a': 'x', 'b': 'x'}]
        self.assertEquals(expected, result)


    def testExplodeTemplateVarsArray(self):
        from datetime import datetime, timedelta

        n = datetime.today()
        expectedDt = [dt.strftime("%Y%m%d") for dt in [n, n + timedelta(
                days=-1)]]
        requestedKeys = set(['folder', 'foo', 'yyyymmdd'])
        template = {"folder": "afolder",
                    "foo": "bar_{folder}_{filename}",
                    "yyyymmdd": [-1, 0]}
        result = explodeTemplateVarsArray(requestedKeys, [template],
                                          {"dataset": "adataset",
                                           "project": "aproject",
                                           "filename": "afile"})
        expected = [{'filename': 'afile', 'folder': 'afolder', 'dataset':
                    'adataset', 'yyyymmdd': expectedDt[1], 'foo':
                    'bar_afolder_afile', "table": "afile",
                     "project": "aproject"},
                    {'filename': 'afile', 'folder': 'afolder', 'dataset':
                    'adataset', 'yyyymmdd': expectedDt[0], 'foo':
                        'bar_afolder_afile', "table": "afile",
                     "project": "aproject"}]
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
