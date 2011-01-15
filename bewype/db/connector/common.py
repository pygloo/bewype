# (C) Copyright 2010 Bewype <http://www.bewype.org>

# sqlite3 import
from sqlite3 import Cursor as sqli3_cursor
from MySQLdb.cursors import Cursor as mysql_cursor

class ExecuteError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class InvalidClauseType(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class WhereClause(object):

    def __init__(self, type_, criteria_list):
        self.type_ = type_
        self.criteria_list = criteria_list

    def serialize(self):
        if self.type_ in ['and', 'or']:
            return (' %s ' % self.type_).join(
                    ['(%s)' % _c.serialize() if isinstance(_c, WhereClause)\
                            else _c for _c in self.criteria_list])
        else:
            raise InvalidClauseType('Invalid type %s with criteria: %s'\
                    % (self.type_, self.criteria_list))


class and_(WhereClause):

    def __init__(self, criteria_list):
        WhereClause.__init__(self, 'and', criteria_list)


class or_(WhereClause):

    def __init__(self, criteria_list):
        WhereClause.__init__(self, 'or', criteria_list)


def _select_query(db, table_name, column_list=None, where=None, order_list=None,
        order='asc', into=None):
    # prepare query
    _q = 'select %s' % (', '.join(column_list)
                        if isinstance(column_list, list) else '*')
    _q = '%s from %s' % (_q, table_name)
    # set where clause
    if where is None:
        pass
    else:
        _q = '%s where %s' % (_q, where.serialize())
    # set order by ckause
    if order_list is None:
        pass
    else:
        _q = '%s order by %s %s' % (_q, ', '.join(order_list), order)
    # add into params
    if into is None:
        _q = '%s;' % _q
    else:
        _q = '%s into %s;' % (_q, into)
    # trigger query
    _cursor = db.cursor()
    # execute query
    _r = _cursor.execute(_q)
    # return db and cursor for additional work
    return _cursor, _r


def count(db, table_name, where=None):
    # get result and db for closing
    _cursor, _count = _select_query(db, table_name, where=where)
    # simple count factory
    if isinstance(_cursor, sqli3_cursor):
        _count = len(_cursor.fetchall())
    elif isinstance(_cursor, mysql_cursor):
        _count = _cursor.rowcount
    else:
        _count = None
    # close the connection
    _cursor.close()
    # return count
    return _count


def create_table(db, table_name, column_defs):
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
    # prepare query
    _q = 'create table %s' % table_name
    _q = '%s (%s);' % (_q, ', '.join([' '.join(_d) for _d in column_defs]))
    # return count
    return execute(db, _q)


def drop_table(db, table_name):
    # prepare query
    _q = 'drop table %s;' % table_name
    # return count
    return execute(db, _q)


def delete(db, table_name, where=None):
    # prepare query
    _q = 'delete'
    _q = '%s from %s' % (_q, table_name)
    # criteria or not
    if where is None:
        _q = '%s;' % _q
    else:
        _q = '%s where %s;' % (_q, where.serialize())
    # return count
    return execute(db, _q)


def execute(db, query):
    # get cursor
    _cursor   = db.cursor()
    _in_error = False
    # do it
    try:
        # execute
        _result = _cursor.execute(query)
        # commit your changes in the database
        db.commit()
    except Exception, e:
        # ensure _result var
        _in_error = True
        _result   = e
        # rollback in case there is any error
        db.rollback()
    # close the connection
    _cursor.close()
    # return result or raise an explicit error
    if _in_error:
        raise ExecuteError(_result)
    else:
        return _result


def insert(db, table_name, column_list, values):
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
    raise ExecuteError('insert should use specific connector')


def _result(row, column_list=None):
    # list all
    if column_list is None:
        return row
    elif len(column_list) == 1:
        return row[0]
    else:
        return dict(zip(column_list, [_v for _v in row]))

def select(db, table_name, column_list=None, where=None, order_list=None,
        order='asc'):
    # get result and db for closing
    _cursor, _r = _select_query(db, table_name, column_list=column_list,
                    where=where, order_list=order_list, order=order)
    # sqlite cursor management
    if isinstance(_r, sqli3_cursor):
        _cursor = _r
    else:
        pass
    # list all
    _all = [ _result(_r, column_list=column_list) for _r in _cursor.fetchall()]
    # close the connection
    _cursor.close()
    # return result list
    return _all

def eager_select(db, table_name, column_list=None, where=None, order_list=None,
        order='asc'):
    """!!!If you stop the eager select stuff.. please do not forget to CLOSE the
    connetion using the returned CURSOR ;)!!!
    """
    # get result and db for closing
    _cursor, _r = _select_query(db, table_name, column_list=column_list,
            where=where, order_list=order_list, order=order)
    # sqlite cursor management
    if isinstance(_r, sqli3_cursor):
        _cursor = _r
    else:
        pass
    # first row
    _n = _cursor.fetchone()
    if _n is None:
        # close the connection
        _cursor.close()
        yield None, None
    else:
        while(_n):
            _cur = _n
            _n = _cursor.fetchone()
            if _n is None:
                # close the connection
                _cursor.close()
            # yield it
            yield _cursor, _result(_cur, column_list=column_list)


def update(db, table_name, column_list, values, uri=None, where=None):
    # prepare query
    _q = 'update %s' % table_name
    # prepare set
    _set = ['%s = \'%s\'' % (_c, _v) for _c, _v in zip(column_list, values)]
    # update query
    _q = '%s set %s' % (_q, ', '.join(_set))
    # criteria or not
    if where is None:
        _q = '%s;' % _q
    else:
        _q = '%s where %s;' % (_q, where.serialize())
    # return count
    return execute(db, _q)
