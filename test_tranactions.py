import mysql.connector
import unittest
import datetime
from decimal import Decimal
import pandas as pd
from pandas.testing import assert_frame_equal

from tranactions import (
    get_transactions,
    get_formula_data,
    compute_aggregate
    )
assert_frame_equal

class TestTransction(unittest.TestCase):
    """
    The class with test methods for every part of transaction functions
    """

    def test_compute_aggregate(self):
        """
        functional test of function compute_aggregate
        the output should be equal to the expected dataframe in two dates(2008-Q1, 2008-Q2)
        """

        expected = pd.DataFrame([35.957177, 180.247177], columns = ["Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N"], index = ["2008-Q1", "2008-Q2"])
        expected.index.name = "TIME_PERIOD"

        sample = """Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N
        = Q.N.I8.W1.S1P.S1.T.A.FA.D.F._Z.EUR._T._X.N - Q.N.I8.W1.S1Q.S1.T.A.FA.D.F._Z.EUR._T._X.N"""
        result_df = compute_aggregate(sample)

        assert_frame_equal(expected, result_df.loc[["2008-Q1","2008-Q2"],:])

        

    




if __name__ == '__main__':
    unittest.main()