# -*- coding: UTF-8 -*-
import os
import unittest

from oracle2mysql import OracleAdapter


class MyTest(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    @classmethod
    def setUpClass(cls):
        print('setUpClass')

    def setUp(self):
        print('setUp')

    def tearDown(self):
        print('tearDown')

    def test_a_run(self):
        self.assertEqual(1, 1)

    # OracleAdapter Test
    def test_b_run(self):
        class MockCursor(object):
            def __init__(self):
                self.mock_data = []

            def close(self):
                pass

            def execute(self, sql):
                pass

            def __iter__(self):
                return MyIterator(self.mock_data)

        class MyIterator(object):
            def __init__(self, mock_data_list):
                self.mock_data_list = mock_data_list
                self.current = 0

            def next(self):
                if self.current < len(self.mock_data_list):
                    item = self.mock_data_list[self.current]
                    self.current += 1
                    return item
                else:
                    raise StopIteration

            def __iter__(self):
                return self

        class MockConnection(object):
            def __init__(self):
                pass

            def cursor(self):
                return MockCursor()



        connection = MockConnection()

        oracle_adapter = OracleAdapter(connection)

        oracle_adapter.get_data_for_equip("equip_name", "equip_id")

    def test_get_equip_id_and_type(self):
        class MockCursor(object):
            def __init__(self, ut_obj):
                self.mock_data = [
                    [21719, 8001]
                ]
                self.ut_obj = ut_obj

            def close(self):
                pass

            def execute(self, sql):
                expect_sql = u"SELECT EQUIP_ID, EQUIP_TYPE_ID FROM hqliss1.EQ_EQUIP " \
                              u"WHERE equip_name = '门诊楼B1层低配间A1L31柜螺杆机3号PE410R'"
                self.ut_obj.assertEqual(expect_sql, sql)

            def __iter__(self):
                return MyIterator(self.mock_data)

        class MyIterator(object):
            def __init__(self, mock_data_list):
                self.mock_data_list = mock_data_list
                self.current = 0

            def next(self):
                if self.current < len(self.mock_data_list):
                    item = self.mock_data_list[self.current]
                    self.current += 1
                    return item
                else:
                    raise StopIteration

            def __iter__(self):
                return self

        class MockConnection(object):
            def __init__(self, ut_obj):
                self.ut_obj = ut_obj

            def cursor(self):
                return MockCursor(self.ut_obj)

        connection = MockConnection(self)

        oracle_adapter = OracleAdapter(connection)
        equip_name = u'门诊楼B1层低配间A1L31柜螺杆机3号PE410R'
        id_and_type = oracle_adapter.get_equip_id_and_type(equip_name)
        self.assertEqual(21719, id_and_type['id'])
        self.assertEqual(8001, id_and_type['type'])
