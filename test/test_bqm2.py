import unittest
from collections import defaultdict

from bqm2 import DependencyExecutor


class Test(unittest.TestCase):
    def testHandleRetries(self):
        de = DependencyExecutor(set([]), {}, maxRetry=1)
        retries = defaultdict(lambda: 1)
        de.handleRetries(retries, "aKey")

        # do it again which should blow up
        try:
            de.handleRetries(retries, "aKey")
            self.fail("Should have thrown exception")
        except:
            pass


if __name__ == '__main__':
    unittest.main()
