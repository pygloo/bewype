#!/usr/bin/env python
# (C) Copyright 2010 Bewype <http://www.bewype.org>

# python import
import os, unittest

# bewype import
from bewype.config import obj as c
from bewype.db.connector import sqlite, mysql, ExecuteError, and_, or_


def __set_data(connector, uri):
    # params
    _columns = ['name', 'category']
    _values  = [
            ('snake', 'reptile'),
            ('frog', 'amphibian')
            ]
    # check exist
    _r = connector.insert('animal', _columns, _values, uri=uri)
    # check insert
    assert _r == 2, 'insert found: %s' % _r


def _test_create_table(connector, uri):
    # check exist
    _r = connector.exist_table('animal', uri=uri)
    # test
    assert _r is True, 'exist found: %s' % _r

    # check not exist
    _r = connector.exist_table('dummy', uri=uri)
    # test
    assert _r is False, 'exist found: %s' % _r


def _test_count(connector, uri):
    # ...
    __set_data(connector, uri)

    # check count
    _r = connector.count('animal', uri=uri)
    # check count
    assert _r == 2, 'count found: %s' % _r

    # check count
    _r = connector.count('animal', where=and_(['name = \'snake\'']), uri=uri)
    # check count
    assert _r == 1, 'count found: %s' % _r


def _test_update(connector, uri):
    # ...
    __set_data(connector, uri)

    # udpate something
    _r = connector.update('animal', column_list=['category'], values=['test'],
            where=and_(['name = \'frog\'']), uri=uri)

    # check count
    _r = connector.count('animal', where=and_(['category = \'test\'']), uri=uri)
    # check count
    assert _r == 1, 'count found: %s' % _r

    # check count
    _r = connector.count('animal', where=and_(['category = \'amphibian\'']), uri=uri)
    # check count
    assert _r == 0, 'count found: %s' % _r


def _test_select(connector, uri):
    # ...
    __set_data(connector, uri)

    # check select
    _r = connector.select('animal', column_list=['category'],
            order_list=['category'], uri=uri)
    # check select
    assert _r == [u'amphibian', u'reptile'], 'select found: %s' % _r

    # check select
    _r = connector.select('animal', column_list=['category'],
            order_list=['category'], order='desc', uri=uri)
    # check select
    assert _r == [u'reptile', u'amphibian'], 'select found: %s' % _r

    # check select
    _r = connector.select('animal', column_list=['name'],
            where=and_(['name=\'snake\'', 'category=\'reptile\'']), uri=uri)
    # check select
    assert _r == [u'snake'], 'select found: %s' % _r

    # check select
    _r = connector.select('animal', column_list=['category'],
            where=or_(['name=\'snake\'', 'name=\'frog\'']), uri=uri)
    # check select
    assert _r == [u'reptile', u'amphibian'], 'select found: %s' % _r

    # check eager_select
    _generator = connector.eager_select('animal', column_list=['category'],
            where=or_(['name=\'snake\'', 'name=\'frog\'']), uri=uri)
    # check eager_select
    _all = [_r for _c, _r in _generator]
    assert _all == [u'reptile', u'amphibian'],\
            'eager_select found: %s' % _all


def _test_delete(connector, uri):
    # ...
    __set_data(connector, uri)

    # check delete
    _r = connector.delete('animal', where=and_(['name = \'frog\'']), uri=uri)
    # check delete
    assert _r == 1, 'delete found: %s' % _r

    # check select
    _r = connector.select('animal', column_list=['category'],
            where=and_(['name = \'snake\'']), uri=uri)
    # check select
    assert _r == [u'reptile'], 'select found: %s' % _r

    # check delete all
    _r = connector.delete('animal', uri=uri)
    # check delete
    assert _r == 1, 'delete found: %s' % _r

    # check count
    _r = connector.count('animal', uri=uri)
    # check count
    assert _r == 0, 'count found: %s' % _r


def _test_drop_table(connector, uri):
    # drop existing table
    _r = connector.drop_table('animal', uri=uri)
    # test
    assert _r is True, 'drop_table found: %s' % _r

    # drop non-existing table
    try:
        connector.drop_table('dummy', uri=uri)
        # ??
        assert False, 'should raise an error on drop'
    except Exception, e:
        # test
        assert isinstance(e, ExecuteError), 'drop error found: %s' % e


class TestSQLite(unittest.TestCase):

    def setUp(self):
        # main uri
        self._uri = 'test_connector.db'
        # ...
        _column_defs = [
            ('name',     'char(40)'),
            ('category', 'char(40)')
        ]
        # create table for our tests
        _r = sqlite.create_table('animal', _column_defs, uri=self._uri)
        # ..
        assert _r is True, 'found %s on create table' % _r

    def tearDown(self):
        # check exist
        _r = sqlite.exist_table('animal', uri=self._uri)
        if _r is True:
            # drop table for our tests
            sqlite.drop_table('animal', uri=self._uri)
        # remove test file at the end
        os.remove('test_connector.db')

    def test_create_table(self):
        _test_create_table(sqlite, self._uri)

    def test_count(self):
        _test_count(sqlite, self._uri)

    def test_update(self):
        _test_update(sqlite, self._uri)

    def test_select(self):
        _test_select(sqlite, self._uri)

    def test_delete(self):
        _test_delete(sqlite, self._uri)

    def test_drop_table(self):
        _test_drop_table(sqlite, self._uri)


class TestMySQL(unittest.TestCase):

    def setUp(self):
        # main uri
        self._uri = c.db.db_uri
        # check db exist
        if not mysql.exist_db(db_name='test_db'):
            print 'create test db...'
            mysql.create_db(db_name='test_db')
            print 'create db ok!'
        # ...
        _column_defs = [
            ('name',     'char(40)'),
            ('category', 'char(40)')
        ]
        # create table for our tests
        _r = mysql.create_table('animal', _column_defs, uri=self._uri)
        # ..
        assert _r is True, 'found %s on create table' % _r

    def tearDown(self):
        # check exist
        _r = mysql.exist_table('animal', uri=self._uri)
        if _r is True:
            # drop table for our tests
            mysql.drop_table('animal', uri=self._uri)

    def test_create_table(self):
        _test_create_table(mysql, self._uri)

    def test_count(self):
        _test_count(mysql, self._uri)

    def test_update(self):
        _test_update(mysql, self._uri)

    def test_select(self):
        _test_select(mysql, self._uri)

    def test_delete(self):
        _test_delete(mysql, self._uri)

    def test_drop_table(self):
        _test_drop_table(mysql, self._uri)


if __name__ == '__main__':
    unittest.main()

