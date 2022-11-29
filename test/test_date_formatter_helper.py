import json
import unittest
import logging

import date_formatter_helper
import iter_util


class Test(unittest.TestCase):

    def test_formatters_bad_date_throw_exception(self):
        try:
            date_formatter_helper.helpers.format_all_date_keys({"yyyymm": "aaa"})
        except ValueError:
            pass

    def test_formatters_show_new_keys(self):
        self.assertEqual(set(["yyyymm_yyyy", "yyyymm_mm"]), date_formatter_helper.helpers.show_new_keys(["yyyymm"]))
        self.assertEqual(set(["foo_yyyymm_yyyy", "foo_yyyymm_mm"]),
                         date_formatter_helper.helpers.show_new_keys(["foo_yyyymm"]))
        self.assertEqual(set(["yyyymmdd_yyyy", "yyyymmdd_mm", "yyyymmdd_dd", "yyyymmdd_yy"]),
                         date_formatter_helper.helpers.show_new_keys(["yyyymmdd"]))
        self.assertEqual(set(["foo_yyyymmdd_yyyy", "foo_yyyymmdd_mm", "foo_yyyymmdd_dd", "foo_yyyymmdd_yy"]),
                         date_formatter_helper.helpers.show_new_keys(["foo_yyyymmdd"]))
        self.assertEqual(set(["yyyymmddhh_yyyy", "yyyymmddhh_mm", "yyyymmddhh_dd", "yyyymmddhh_hh"]),
                         date_formatter_helper.helpers.show_new_keys(["yyyymmddhh"]))
        self.assertEqual(set(["foo_yyyymmddhh_yyyy", "foo_yyyymmddhh_mm", "foo_yyyymmddhh_dd", "foo_yyyymmddhh_hh"]),
                         date_formatter_helper.helpers.show_new_keys(["foo_yyyymmddhh"]))

    def test_formatters_format_all_keys(self):
        inp = {
            "yyyy": "2022",
            "yyyymm": "202212",
            "yyyymmdd": "20221231",
            "yyyymmddhh": "2022123101",
            "foo_yyyy": "2022",
            "foo_yyyymm": "202212",
            "foo_yyyymmdd": "20221231",
            "foo_yyyymmddhh": "2022123101",
        }

        expected = {
          "yyyy": "2022",
          "yyyymm": "202212",
          "yyyymmdd": "20221231",
          "yyyymmddhh": "2022123101",
          "foo_yyyy": "2022",
          "foo_yyyymm": "202212",
          "foo_yyyymmdd": "20221231",
          "foo_yyyymmddhh": "2022123101",
          "yyyymmddhh_yyyy": "2022",
          "yyyymmddhh_mm": "12",
          "yyyymmddhh_dd": "31",
          "yyyymmddhh_hh": "01",
          "foo_yyyymmddhh_yyyy": "2022",
          "foo_yyyymmddhh_mm": "12",
          "foo_yyyymmddhh_dd": "31",
          "foo_yyyymmddhh_hh": "01",
          "yyyymmdd_yyyy": "2022",
          "yyyymmdd_yy": "22",
          "yyyymmdd_mm": "12",
          "yyyymmdd_dd": "31",
          "foo_yyyymmdd_yy": "22",
          "foo_yyyymmdd_yyyy": "2022",
          "foo_yyyymmdd_mm": "12",
          "foo_yyyymmdd_dd": "31",
          "yyyymm_yyyy": "2022",
          "yyyymm_mm": "12",
          "foo_yyyymm_yyyy": "2022",
          "foo_yyyymm_mm": "12"
        }

        date_formatter_helper.helpers.format_all_date_keys(inp)
        self.assertEqual(inp, expected)

if __name__ == '__main__':
    import sys
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    unittest.main()
