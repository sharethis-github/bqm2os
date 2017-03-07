import unittest

from rsrc.Rsrc import strictSubstring, legacyBqQueryDependsOn, Resource


class Test(unittest.TestCase):
    def test_strictSubstring(self):
        self.assertTrue(strictSubstring("A", "AA"))
        self.assertFalse(strictSubstring("A", "A"))
        self.assertTrue(strictSubstring("A", " Asxx "))

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

    def test_DatasetDependency(self):
        resource = Resource()
        def key():
            return "mergelog"
        resource.key = key

        _self = Resource()
        def s():
            return "mergelog.table"
        _self.key = s
        self.assertTrue(legacyBqQueryDependsOn(_self, resource,
                                                "somequery"))
        self.assertFalse(legacyBqQueryDependsOn(resource, _self,
                                                "somequery"))
