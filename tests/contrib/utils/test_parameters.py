from __future__ import absolute_import

import unittest
from airflow.contrib.utils.parameters import (
    XComParameter,
    enumerate_parameter_dict,
)


class MockXCom:
    def __init__(self):
        self._dict = {}

    def set(self, task_id, key, value):
        task_dict = self._dict.get(task_id)
        if not task_dict:
            task_dict = {}
            self._dict[task_id] = task_dict
        task_dict[key] = value

    def get_safe(self, task_id, key):
        task_dict = self._dict.get(task_id)
        if not task_dict:
            return None
        return task_dict.get(key)

    def xcom_pull(
        self,
        task_ids=None,
        dag_id=None,
        key='return_value',
        include_prior_dates=False
    ):
        if hasattr(task_ids, '__iter__') and not isinstance(task_ids, basestring):
            return [self.get_safe(t, key) for t in task_ids]
        else:
            return self.get_safe(task_ids, key)


class XComParameterTest(unittest.TestCase):
    def test_get_single(self):
        xcom = MockXCom()
        xcom.set('t1', 'return_value', 'v')
        xcom.set('t2', 'return_value', 'w')
        xcp = XComParameter('t1')
        retval = xcp.get_values(xcom)
        assert retval
        assert 1 == len(retval)
        assert 'v' == retval[0]

    def test_get_single_list(self):
        xcom = MockXCom()
        xcom.set('t1', 'return_value', 'v')
        xcom.set('t2', 'return_value', 'w')
        xcp = XComParameter(['t1'])
        retval = xcp.get_values(xcom)
        assert retval
        assert 1 == len(retval)
        assert 'v' == retval[0]

    def test_get_multi(self):
        xcom = MockXCom()
        xcom.set('t1', 'return_value', 'v')
        xcom.set('t2', 'return_value', 'w')
        xcp = XComParameter(['t1', 't2'])
        retval = xcp.get_values(xcom)
        assert retval
        assert 2 == len(retval)
        assert 'v' == retval[0]
        assert 'w' == retval[1]

    def test_get_multi_missing(self):
        xcom = MockXCom()
        xcom.set('t1', 'return_value', 'v')
        xcom.set('t2', 'return_value', 'w')
        xcp = XComParameter(['t1', 'hrm', 't2'])
        retval = xcp.get_values(xcom)
        assert retval
        assert 3 == len(retval)
        assert 'v' == retval[0]
        assert retval[1] is None
        assert 'w' == retval[2]


class EnumerateParameterDictTest(unittest.TestCase):
    def test_empty(self):
        assert 0 == len(list(enumerate_parameter_dict({}, MockXCom())))

    def test_simple(self):
        result = set([
            "%s|%s" % (k, v)
            for k, v
            in enumerate_parameter_dict(dict(foo='bar', baz='biff'), MockXCom())
        ])
        assert 2 == len(result)
        assert "foo|bar" in result
        assert "baz|biff" in result

    def test_list(self):
        result = set([
            "%s|%s" % (k, v)
            for k, v
            in enumerate_parameter_dict(dict(foo=['bar', 'baz']), MockXCom())
        ])
        assert 2 == len(result)
        assert "foo|bar" in result
        assert "foo|baz" in result

    def test_dict(self):
        result = set([
            "%s|%s" % (k, v)
            for k, v
            in enumerate_parameter_dict(dict(foo=dict(bar='baz', qux='plough')), MockXCom())
        ])
        assert 2 == len(result)
        assert "foo|bar=baz" in result
        assert "foo|qux=plough" in result

    def test_xcoms(self):
        xcom = MockXCom()  # this is will stand-in for the TaskInstance
        xcom.set('t1', 'return_value', 'v')
        xcom.set('t2', 'return_value', 'w')
        xcp = XComParameter(['t1', 'hrm', 't2'])

        result = set([
            "%s|%s" % (k, v)
            for k, v
            in enumerate_parameter_dict(dict(foo=xcp), xcom)
        ])

        assert 2 == len(result)
        assert "foo|v" in result
        assert "foo|w" in result
