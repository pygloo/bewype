# (C) Copyright 2010 Bewype <http://www.bewype.org>

# sqlite3 import
import sqlite3

# bewype import
from bewype.config import obj as c
from bewype.db.connector import common


def _uri(uri=None):
    # ensure uri
    return c.db.db_uri\
            if uri is None else uri


def _close(cursor):
    # little check
    if isinstance(cursor, sqlite3.Cursor):
        cursor.close()
        return True
    else:
        raise common.ExecuteError(['invalid result', c])


def count(table_name, uri=None, where=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return common.count(_db, table_name, where=where)


def create_table(table_name, column_defs, uri=None):
    """Create table to the current db according to the passed table name and
    column definitions, ex.::

        # params
        _column_defs = [
            ('name',     'char(40)'),
            ('category', 'char(40)'),
            ...
        ]
        # create_table
        create_table('animal', _column_defs)

    """
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return _close(common.create_table(_db, table_name, column_defs))


def drop_table(table_name, uri=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return _close(common.drop_table(_db, table_name))


def delete(table_name, uri=None, where=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    _c = common.delete(_db, table_name, where=where)
    # check number of removed rows
    _count = _c.rowcount
    # close the connection
    _close(_c)
    # return result
    return _count


def execute(query, uri=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return common.execute(_db, query)


def exist_table(table_name, uri=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # prepare params
    _column_list = ['name']
    _criteria_list = [
            'type = \'table\'',
            'name = \'%s\'' % table_name,
            ]
    # count result
    _r = common.select(_db, 'sqlite_master', column_list=_column_list,
            where=common.and_(_criteria_list))
    # return test
    return len(_r) != 0


def insert(table_name, column_list, values, uri=None):
    """Insert value in the giaven db and table assuming column list corresponds
    with past list of rows to add, ex.::

        # params
        _columns = ['name', 'category']
        _values  = [('snake', 'reptile'), ('frog', 'amphibian'), ...]
        # insert
        insert('animal', _columns, _values)

    Corresponding SQL query::

        INSERT INTO animal (name, category)
        VALUES
            ('snake', 'reptile'),
            ('frog', 'amphibian'),
            ('tuna', 'fish'), ...;

    """
    # int count result
    _count = 0
    for _r in values:
        # prepare query
        _q = 'insert into %s (%s)' % (table_name, ', '.join(column_list))
        _q = '%s values (\'%s\');' % (_q, '\', \''.join([str(_v) for _v in _r]))
        # insert a row
        _c = execute(_q, uri=uri)
        # update count
        _count += _c.rowcount
        # close
        _close(_c)
    # return count
    return _count


def select(table_name, uri=None, column_list=None, where=None, order_list=None,
        order='asc'):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return common.select(_db, table_name, column_list=column_list, where=where,
            order_list=order_list, order=order)


def eager_select(table_name, uri=None, column_list=None, where=None,
        order_list=None, order='asc'):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return common.eager_select(_db, table_name, column_list=column_list,
            where=where, order_list=order_list, order=order)


def update(table_name, column_list, values, uri=None, where=None):
    # parse value from uri
    _db = sqlite3.connect(_uri(uri=uri))
    # return common
    return common.update(_db, table_name, column_list, values, uri=uri,
            where=where)
