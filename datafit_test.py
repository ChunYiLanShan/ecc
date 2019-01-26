# -*- coding: UTF-8 -*-
import unittest
import datafit


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

    # OracleAdapter Test
    def test_fit_energy_data(self):
        fit_tool = datafit.FittingTool()
        energy_data_hist = [

        ]
        fitted_data = fit_tool.fit_energy_data(energy_data_hist)

    def test_fit_data(self):
        # Valid cases
        self._fit_data_case(4, [1, 2, 3])
        self._fit_data_case(4.0, [1.0, 2.0, 3.0])
        self._fit_data_case(1, [1, 1])
        self._fit_data_case(1, [1])

        # Exception case
        self._fit_data_exception_case(AssertionError, [])

    def _fit_data_case(self, expected, hist_data):
        fit_tool = datafit.FittingTool()
        fitted_data = fit_tool.fit_data(hist_data)
        self.assertEqual(expected, fitted_data)

    def _fit_data_exception_case(self, exception, bad_hist_data):
        with self.assertRaises(exception) as context:
            self._fit_data_case(-1, [])
        self.assertEqual(exception, type(context.exception))

