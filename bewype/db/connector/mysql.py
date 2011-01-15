# (C) Copyright 2010 Bewype <http://www.bewype.org>

# python import
import os

# mysql import
import MySQLdb

# bewype import
from bewype.config import obj as c
from bewype.db.connector import common

# logging
import logging
log = logging.getLogger(__name__)


def split_uri(uri=None):
    uri = c.db.db_uri if uri is None else uri
    # split sql
    _t = uri.split('/')
    if len(_t) != 4:
        return None
    # get table name
    _db = _t[-1]
    _user_pwd, _host_port = _t[2].split('@')
    # ...
    _host = _host_port.split(':')[0] if ':' in _host_port else _host_port
    _port = _host_port.split(':')[1] if ':' in _host_port else None
    _user, _pwd = _user_pwd.split(':')
    # connction dict
    _arg = {'host': _host,
            'user': _user,
            'passwd': _pwd,
            'db': _db}
    # set port
    if _port is None:
        pass
    else:
        _arg['port'] = int(_port)
    # return parsed args
    return _arg


def count(table_name, uri=None, where=None):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.count(_db, table_name, where=where)


def create_db(user=None, pwd=None, host=None, port=None, db_name=None):
    # prepare params
    _user = c.db.db_user if user is None else user
    _pwd = c.db.db_pass if pwd is None else pwd
    _host = c.db.db_host if host is None else host
    _db_name = c.db.db_name if db_name is None else db_name
    # do create
    _db = MySQLdb.connect(user=_user, passwd=_pwd, host=_host)
    _c = _db.cursor()
    _c.execute('create database %s;' % _db_name)
    _c.close()


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
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.create_table(_db, table_name, column_defs) == 0


def drop_table(table_name, uri=None):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.drop_table(_db, table_name) == 0


def delete(table_name, uri=None, where=None):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.delete(_db, table_name, where=where)


def execute(query, uri=None):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.execute(_db, query)


def exist_db(user=None, pwd=None, host=None, db_name=None):
    # prepare params
    _user = c.db.db_user if user is None else user
    _pwd = c.db.db_pass if pwd is None else pwd
    _host = c.db.db_host if host is None else host
    _db_name = c.db.db_name if db_name is None else db_name
    # Get a list of databases with :
    _cmd = "mysql -u %s -p%s -h %s --silent -N -e 'show databases'"\
            % (_user, _pwd, _host)
    for _db in os.popen(_cmd).readlines():
        _db = _db.strip()
        if _db == 'information_schema':
            continue
        elif _db == _db_name:
            return True
        else:
            continue
    # not found
    return False


def exist_table(table_name, uri=None):
    # prepare exist query
    _q = 'show tables like \'%s\'' % table_name
    # execute query
    _r = execute(_q, uri=uri)
    # return test
    return _r != 0


def export_(table_name, into, uri=None, column_list=None, where=None,
        order_list=None, order='asc'):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # get result and db for closing
    _cursor, _r = common._select_query(_db, table_name, column_list=column_list,
                    where=where, order_list=order_list, order=order, into=into)
    # close the connection
    _cursor.close()


def import_(table_name, infile, column_list, uri=None):
    # prepare query
    _q = 'load data infile \'%s\'' % infile
    _q = '%s into table %s (%s);' % (_q, table_name, ', '.join(column_list))
    # return count
    return execute(_q, uri=uri)


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
    # prepare query
    _q = 'insert into %s (%s)' % (table_name, ', '.join(column_list))
    _q = '%s values (\'%s\');' % (_q,
            '\'), (\''.join(['\', \''.join(_r) for _r in values]))
    # return count
    return execute(_q, uri=uri)


def select(table_name, uri=None, column_list=None, where=None, order_list=None,
        order='asc'):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.select(_db, table_name, column_list=column_list, where=where,
            order_list=order_list, order=order)


def eager_select(table_name, uri=None, column_list=None, where=None,
        order_list=None, order='asc'):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.eager_select(_db, table_name, column_list=column_list,
            where=where, order_list=order_list, order=order)


def update(table_name, column_list, values, uri=None, where=None):
    # parse value from uri
    _db = MySQLdb.connect(**split_uri(uri=uri))
    # return common
    return common.update(_db, table_name, column_list, values, uri=uri,
            where=where)
