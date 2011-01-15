#!/usr/bin/env python
# (C) Copyright 2010 Bewype <http://www.bewype.org>

# python import
import os, unittest

# bewype import
from bewype.db.connector import and_, or_


class TestWhere(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_and(self):
        # build where clause
        _and = and_(['name = \'snake\'', 'category = \'reptile\''])
        # serialize
        _c = _and.serialize()
        # test
        assert _c == 'name = \'snake\' and category = \'reptile\'',\
                'and_ found: %s' % _c

    def test_or(self):
        _or = or_(['name = \'snake\'', 'name = \'frog\''])
        # serialize
        _c = _or.serialize()
        # test
        assert _c == 'name = \'snake\' or name = \'frog\'',\
                'or_ found: %s' % _c

    def test_mixed(self):
        # build where clause
        _and = and_(['name = \'snake\''])
        _or = or_(['category = \'reptile\'', 'category = \'amphibian\''])
        _mixed = and_([_and, _or])
        # serialize
        _c = _mixed.serialize()
        # test
        assert _c == '(name = \'snake\') and'\
                ' (category = \'reptile\' or category = \'amphibian\')',\
                'or_ found: %s' % _c


if __name__ == '__main__':
    unittest.main()

