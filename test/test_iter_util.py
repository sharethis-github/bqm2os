import unittest

import iter_util

class Test(unittest.TestCase):

    def testBasicIteration(self):
        expected = set(range(2))
        accum = set()
        data = [x for x in range(2)]
        def v(x):
            popped = data.pop()
            accum.add(popped)
            return True

        def g(iter=None):
            if iter is not None:
                return len(data) and data or None
            return data

        iter_util.iterate(visitorFunc=v, iterFunc=g)
        print(expected)
        self.assertEqual(accum, expected)


    def testMultiplePages(self):

        accum = []
        expected = [True, True, False]

        data = [True, False, True, True]
        def v(x):
            popped = data.pop()
            accum.append(popped)
            return popped

        def g(iter=None):
            if iter:
                return len(iter) and iter or None
            return data

        iter_util.iterate(v, g)
        self.assertEqual(accum, expected)


if __name__ == '__main__':
    unittest.main()
