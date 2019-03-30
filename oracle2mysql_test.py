# -*- coding: utf-8 -*-

import unittest

from oracle2mysql import OracleAdapter


class MyTest(unittest.TestCase):

    # OracleAdapter Test
    def test_check_point_type(self):

        # Normal cases
        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R-有功功耗',
            expected_is_got_type=True,
            expected_type='quantity'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R-有功功耗-中',
            expected_is_got_type=False,
            expected_type=None
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - 有功功耗(虚点)',
            expected_is_got_type=False,
            expected_type=None
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - 有功功率',
            expected_is_got_type=True,
            expected_type='power'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - A相电流',
            expected_is_got_type=True,
            expected_type='current_A'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - B相电流',
            expected_is_got_type=True,
            expected_type='current_B'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R-C相电流',
            expected_is_got_type=True,
            expected_type='current_C'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - A相电压',
            expected_is_got_type=True,
            expected_type='voltage_A'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - B相电压',
            expected_is_got_type=True,
            expected_type='voltage_B'
        )

        self._case_point_type(
            point_name=None,
            point_desc='儿外科B1层低配间三层PICU电力（备）PE410R - C相电压',
            expected_is_got_type=True,
            expected_type='voltage_C'
        )

        # Case with leading/trailing spaces

        self._case_point_type(
            point_name=None,
            point_desc='  儿外科B1层低配间三层PICU电力（备）PE410R-有功功耗  ',
            expected_is_got_type=True,
            expected_type='quantity'
        )

        self._case_point_type(
            point_name=None,
            point_desc='  儿外科B1层低配间三层PICU电力（备）PE410R - C相电压  ',
            expected_is_got_type=True,
            expected_type='voltage_C'
        )

    def _case_point_type(self, point_name, point_desc, expected_is_got_type, expected_type):
        row_dict = {
            'NAME': point_name,
            'POINTDESC': point_desc
        }

        got_type, type_val = OracleAdapter.check_point_type(row_dict)
        self.assertEqual(expected_is_got_type, got_type)
        self.assertEqual(expected_type, type_val)