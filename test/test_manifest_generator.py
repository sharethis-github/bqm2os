import json
import logging
import sys
import unittest

from unittest import mock
from google.cloud.storage import Client, Blob, Bucket

from manifest_generator import * 


class Test(unittest.TestCase):
    def test_split_uri(self):
        bucket, prefix = split_uri("s3://bucket/prefix/")
        self.assertEqual(bucket, "bucket")
        self.assertEqual(prefix, "prefix/")

    def test_bad_input(self):
        manifest_path = "s3://bucket/prefix/manifest/"
        try:
            generate_manifest("s3://bucket/prefix/", manifest_path, manifest_path, True)
            self.fail(f"The manifest-path argument provided ({manifest_path}) cannot end with a trailing slash")
        except:
            pass

    def test_entries(self):
        suffix = "txt"
        blobs = ["file.txt"]
        res = create_entries(blobs, suffix)
        self.assertEqual(res['entries'][0]['meta']['content_length'], 1)
        self.assertEqual(res['entries'][0]['mandatory'], True)
        self.assertEqual(res['entries'][0]['url'], f"s3://bucket/file.txt")

    def test_string(self):
        output_dict = {
            "entries": [
                {
                    "meta": 1,
                    "mandatory": True,
                    "url": "s3//bucket/manifest"
                }
            ]
        }
        output_dict_string = json.dumps(output_dict, sort_keys=True)
        self.assertEqual(output_dict_string, '{"entries": [{"mandatory": true, "meta": 1, "url": "s3//bucket/manifest"}]}')


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    unittest.main()