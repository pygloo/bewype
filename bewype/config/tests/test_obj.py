#!/usr/bin/env python
# (C) Copyright 2010 Bewype <http://www.bewype.org>

# python import
import os, unittest

# bewype import
from bewype.config import c


class TestConfigObj(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_obj(self):
        assert c.main.debug.as_bool() is True,\
                'main - found: %s' % c.main.debug
        assert c._test.dummy.as_list() == ['x', 'y', 'z'],\
                '_test - found: %s' % c._test.dummy


if __name__ == '__main__':
    unittest.main()

