# -*- coding: UTF-8 -*-
import unittest
import datafit
import oracle2mysql


class FitToolTest(unittest.TestCase):

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

    def test_fit_all(self):
        class MockMysqlAdapter(oracle2mysql.MySqlAdatper):
            def set_ut_obj(self, ut_obj):
                self.ut_obj = ut_obj

            def get_circuit_ids(self):
                return [1, 2, 3]

            def get_hist_electricity_circuit(self, circuit_id, latest_count):
                return [

                        FitToolTest._create_enery_data(
                            voltage_A=1.0,
                            voltage_B=1.0,
                            voltage_C=1.0,
                            current_A=1.0,
                            current_B=1.0,
                            current_C=1.0,
                            power=1.0,
                            quantity=1.0,
                            mysql_equip_id=1
                        ),
                        FitToolTest._create_enery_data(
                            voltage_A=2.0,
                            voltage_B=2.0,
                            voltage_C=2.0,
                            current_A=2.0,
                            current_B=2.0,
                            current_C=2.0,
                            power=2.0,
                            quantity=2.0,
                            mysql_equip_id=1
                        )
                    ]

            def get_all_equip_names(self):
                return [
                    {'id': 1, 'name':'circuit_name_1'},
                    {'id': 2, 'name': 'circuit_name_2'},
                    {'id': 3, 'name': 'circuit_name_2'}
                ]

            def insert_energy_point_data_in_batch(self, energy_data_list):
                # print energy_data_list[0].__dict__
                first_element = energy_data_list[0]
                self.ut_obj.assertEqual(1, first_element.mysql_equip_id)
                self.ut_obj.assertEqual(3.0, first_element.voltage_A)

                pass

        db_conn = None

        mysql_adapter = MockMysqlAdapter(db_conn)
        mysql_adapter.set_ut_obj(self)
        fit_tool = datafit.FittingTool(mysql_adapter)
        fit_tool.fit_all()

    def test_fit_energy_data(self):
        energy_data_hist = [
            FitToolTest._create_enery_data(
                voltage_A=1.0,
                voltage_B=1.0,
                voltage_C=1.0,
                current_A=1.0,
                current_B=1.0,
                current_C=1.0,
                power=1.0,
                quantity=1.0,
                mysql_equip_id = 1
            ),
            FitToolTest._create_enery_data(
                voltage_A=2.0,
                voltage_B=2.0,
                voltage_C=2.0,
                current_A=2.0,
                current_B=2.0,
                current_C=2.0,
                power=2.0,
                quantity=2.0,
                mysql_equip_id = 1
            )
        ]
        fitted_data = datafit.FittingTool.fit_energy_data(energy_data_hist)
        fields = [
            'voltage_A',
            'voltage_B',
            'voltage_C',
            'current_A',
            'current_B',
            'current_C',
            'power',
            'quantity'
        ]
        for field in fields:
            self.assertEqual(3.0, getattr(fitted_data, field))
        self.assertEqual(1, fitted_data.mysql_equip_id)


    @staticmethod
    def _create_enery_data(**kwargs):
        energy_data = oracle2mysql.EquipEnergyData()
        for k,v in kwargs.items():
            setattr(energy_data, k, v)
        return energy_data

    def test_fit_data(self):
        # Valid cases
        self._fit_data_case(4, [1, 2, 3])
        self._fit_data_case(4.0, [1.0, 2.0, 3.0])
        self._fit_data_case(1, [1, 1])
        self._fit_data_case(1, [1])

        # Exception case
        self._fit_data_exception_case(AssertionError, [])

    def _fit_data_case(self, expected, hist_data):
        fitted_data = datafit.FittingTool.fit_data(hist_data)
        self.assertEqual(expected, fitted_data)

    def _fit_data_exception_case(self, exception, bad_hist_data):
        with self.assertRaises(exception) as context:
            self._fit_data_case(-1, [])
        self.assertEqual(exception, type(context.exception))

