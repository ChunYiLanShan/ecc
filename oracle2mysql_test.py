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

    # OracleAdapter Test
    def test_get_data_for_equip(self):
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

        # 20976--急诊楼B1层低配间手术室空调2PE410R--None--8001--None--


        #Mock methods
        oracle_adapter.get_equip_id_and_type = lambda (name) : {'id':'20892', 'type':'8001'}
        rtm_point_ids = [
            {'point_id': u'310109.XH.8001.20892.12', 'point_type': 'power'},
            {'point_id': u'310109.XH.8001.20892.1', 'point_type': 'current_A'},
            {'point_id': u'310109.XH.8001.20892.10', 'point_type': 'voltage_C'},
            {'point_id': u'310109.XH.8001.20892.16', 'point_type': 'quantity'},
            {'point_id': u'310109.XH.8001.20892.2', 'point_type': 'current_B'},
            {'point_id': u'310109.XH.8001.20892.21', 'point_type': 'quantity'},
            {'point_id': u'310109.XH.8001.20892.3', 'point_type': 'current_C'},
            {'point_id': u'310109.XH.8001.20892.8', 'point_type': 'voltage_A'},
            {'point_id': u'310109.XH.8001.20892.9', 'point_type': 'voltage_B'}
        ]
        oracle_adapter.get_rtm_point_ids = lambda equip_id, equip_type: rtm_point_ids

        def mock_get_point_val_from_rtm_controldata(point_id):
            point_id_to_value = {
                u'310109.XH.8001.20892.12': 12.0,
                u'310109.XH.8001.20892.1': 1.0,
                u'310109.XH.8001.20892.10': 10.0,
                u'310109.XH.8001.20892.16': 16.0,
                u'310109.XH.8001.20892.2':2.0,
                u'310109.XH.8001.20892.21': 21.0,
                u'310109.XH.8001.20892.3':3.0,
                u'310109.XH.8001.20892.8':8.0,
                u'310109.XH.8001.20892.9':9.0

            }
            return point_id_to_value[point_id]

        oracle_adapter._get_point_val_from_rtm_controldata = mock_get_point_val_from_rtm_controldata

        equip_data = oracle_adapter.get_data_for_equip(u"门诊楼B1层低配间空调操作配电室1PE410R", "mysql_equip_id")
        print equip_data
        x = {'current_C': 3.0,
         'name': u'\u95e8\u8bca\u697cB1\u5c42\u4f4e\u914d\u95f4\u7a7a\u8c03\u64cd\u4f5c\u914d\u7535\u5ba41PE410R',
         'current_A': 1.0, 'point_id_to_type': {}, 'voltage_B': 9.0, 'voltage_C': 10.0, 'voltage_A': 8.0, 'power': 12.0,
         'mysql_equip_id': 'mysql_equip_id', 'quatity': None, 'oracle_equip_id': '8001.20892', 'quantity': 21.0,
         'current_B': 2.0}

        self.assertEqual(equip_data.current_A, 1.0)
        self.assertEqual(equip_data.current_B, 2.0)
        self.assertEqual(equip_data.current_C, 3.0)
        self.assertEqual(equip_data.voltage_A, 8.0)
        self.assertEqual(equip_data.voltage_B, 9.0)
        self.assertEqual(equip_data.voltage_C, 10.0)
        self.assertEqual(equip_data.power, 12.0)
        self.assertEqual(equip_data.quantity, 21.0)
        # TODO: correct typo quatity. The typo won't lead to bug, since python is dynamic and my logic
        # will add quantity field dynamically.
        # self.assertEqual(equip_data.quatity, 21.0)
        self.assertEqual(equip_data.oracle_equip_id, '8001.20892')
        self.assertEqual(equip_data.mysql_equip_id, 'mysql_equip_id')
        self.assertEqual(equip_data.name, u"门诊楼B1层低配间空调操作配电室1PE410R")


    def test_get_rtm_point_ids(self):
        class MockCursor(object):
            def __init__(self, ut_obj):
                self.mock_data = [
                    [u'310109.XH.8001.20892.12', u'有功功率', u'20892.12', u'门诊楼B1层低配间空调操作配电室1PE410R-有功功率II',
                        u'8001.20892'],

                    [u"310109.XH.8001.20892.1", u"A相电流", u"20892.1", u"门诊楼B1层低配间空调操作配电室1PE410R-A相电流II",
                        u"8001.20892"],

                    [u"310109.XH.8001.20892.10", u"C相电压", u"20892.10", u"门诊楼B1层低配间空调操作配电室1PE410R-C相电压II",
                        u"8001.20892"],

                    [u"310109.XH.8001.20892.11", u"频率", u"20892.11", u"门诊楼B1层低配间空调操作配电室1PE410R-频率II",
                        u"8001.20892"],

                    [u"310109.XH.8001.20892.12", u"有功功率", u"20892.12", u"门诊楼B1层低配间空调操作配电室1PE410R-有功功率II",
                        u"8001.20892"],

                    [u"310109.XH.8001.20892.13", u"无功功率", u"20892.13", u"门诊楼B1层低配间空调操作配电室1PE410R-无功功率II",
                        u"8001.20892"],

                    [u"310109.XH.8001.20892.14", u"视在功率", u"20892.14", u"门诊楼B1层低配间空调操作配电室1PE410R-视在功率II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.15", u"功率因数", u"20892.15", u"门诊楼B1层低配间空调操作配电室1PE410R-功率因数II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.16", u"有功功耗", u"20892.16", u"门诊楼B1层低配间空调操作配电室1PE410R-有功功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.17", u"无功功耗", u"20892.17", u"门诊楼B1层低配间空调操作配电室1PE410R-无功功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.18", u"视在功耗", u"20892.18", u"门诊楼B1层低配间空调操作配电室1PE410R-视在功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.19", u"开关状态", u"20892.19", u"门诊楼B1层低配间空调操作配电室1PE410R-PE410R-CT II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.2", u"B相电流", u"20892.2", u"门诊楼B1层低配间空调操作配电室1PE410R-B相电流II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.21", u"有功功耗-中", u"20892.21", u"门诊楼B1层低配间空调操作配电室1PE410R-有功功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.25", u"无功功耗-中", u"20892.25", u"门诊楼B1层低配间空调操作配电室1PE410R-无功功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.29", u"视在功耗-中", u"20892.29", u"门诊楼B1层低配间空调操作配电室1PE410R-视在功耗II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.3", u"C相电流", u"20892.3", u"门诊楼B1层低配间空调操作配电室1PE410R-C相电流II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.5", u"A-B线电压", u"20892.5", u"门诊楼B1层低配间空调操作配电室1PE410R-A-B线电压II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.6", u"B-C线电压", u"20892.6", u"门诊楼B1层低配间空调操作配电室1PE410R-B-C线电压II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.7", u"C-A线电压", u"20892.7", u"门诊楼B1层低配间空调操作配电室1PE410R-C-A线电压II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.8", u"A相电压", u"20892.8", u"门诊楼B1层低配间空调操作配电室1PE410R-A相电压II",
                     u"8001.20892"],

                    [u"310109.XH.8001.20892.9", u"B相电压", u"20892.9", u"门诊楼B1层低配间空调操作配电室1PE410R-B相电压II",
                     u"8001.20892"],
                ]

                self.ut_obj = ut_obj
                self.description = [
                    (
                        "POINT_ID",
                    ),
                    (
                        "POINT_NAME",
                    ),
                    (
                        "SHORT_CODE",
                    ),
                    (
                        "DEPICT",
                    ),
                    (
                        "EQUIP_NO",
                    )
                ]

            def close(self):
                pass

            def execute(self, sql):
                expect_sql = u"SELECT point_id, point_name, short_code, depict,equip_no " \
                             u"FROM hqliss1.RTM_POINT WHERE equip_no = '8001.21719'"
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
        conn = MockConnection(self)
        oracle_adapter = OracleAdapter(conn)

        rtm_point_ids = oracle_adapter.get_rtm_point_ids(equip_id=21719, equip_type=8001)
        print rtm_point_ids
        expect = [
            {'point_id': u'310109.XH.8001.20892.12', 'point_type': 'power'},
            {'point_id': u'310109.XH.8001.20892.1', 'point_type': 'current_A'},
            {'point_id': u'310109.XH.8001.20892.10', 'point_type': 'voltage_C'},
            {'point_id': u'310109.XH.8001.20892.12', 'point_type': 'power'},
            {'point_id': u'310109.XH.8001.20892.16', 'point_type': 'quantity'},
            {'point_id': u'310109.XH.8001.20892.2', 'point_type': 'current_B'},
            {'point_id': u'310109.XH.8001.20892.21', 'point_type': 'quantity'},
            {'point_id': u'310109.XH.8001.20892.3', 'point_type': 'current_C'},
            {'point_id': u'310109.XH.8001.20892.8', 'point_type': 'voltage_A'},
            {'point_id': u'310109.XH.8001.20892.9', 'point_type': 'voltage_B'}
        ]
        self.assertEqual(expect, rtm_point_ids)

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
