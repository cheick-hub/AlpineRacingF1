import pandas as pd
import yaml

from bdd.base import BASEBDD
from utils.filters import filter_eq


class FIAV6(BASEBDD):

    def __init__(self, base="FIAMonitoringV6"):
        file = './config/puas.yml'
        with open(file, 'r') as file:
            config = yaml.safe_load(file)
        self._create_engine(config, base)

        # Table construction
        tables = ["FIAMonitoringV6"]
        self._init_table(tables)
        print(base + " DATABASE LOADED")

    def fia_get(self, ssn_list=None):
        t = self.tables
        select = list()
        self._select(select, t.FIAMonitoringV6, ["OCH", "OPH", "TestBenchType"])

        query = self._create_query(select)

        query = filter_eq(query, t.FIAMonitoringV6.c.SSN, ssn_list)
        df = self._read_sql(query)

        r = pd.DataFrame({
            "Total": df[["OCH", "OPH"]].sum(),
            "PUTB": df[df["TestBenchType"] == "PUTB"][["OCH", "OPH"]].sum(),
            "PTTB": df[df["TestBenchType"] == "PTTB"][["OCH", "OPH"]].sum(),
        })

        return r
