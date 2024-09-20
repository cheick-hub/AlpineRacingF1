import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy.orm import sessionmaker

from utils import singleton


class BASEBDD(metaclass=singleton._Singleton):

    def _create_engine(self, config, base):
        connection_uri = url.URL.create(
            "mssql+pyodbc",
            username=config['user'],
            password=config['passwd'],
            host=config['host'],
            database=base,
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "ApplicationIntent": "ReadOnly",
            },
        )
        engine = create_engine(connection_uri)

        self.engine = engine

    def _get_session(self):
        session = sessionmaker(bind=self.engine)
        return session()

    def _create_query(self, select_list):
        session = self._get_session()
        q = session.query(*select_list)
        session.close()
        return q

    def _read_sql(self, q):
        with self.engine.connect() as connection:
            df = pd.read_sql_query(q.selectable, connection)

        return df

    def _execute_sql(self, q, fetch=False):
        """
            Return tuple of the inserted rows if fetch or None.

            Columns to be returned are defined in the 
            table.insert().returning(col1, col2, ...) command
        """
        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(q).fetchall() if fetch else conn.execute(q)
        return res

    def _init_table(self, table_names):
        metadata = MetaData()
        tables = TABLE()
        for table_name in table_names:
            setattr(tables, table_name, Table(table_name, metadata,
                    extend_existing=True, autoload_with=self.engine))

        self.tables = tables

    @staticmethod
    def _select(select, table, columns=None, keep_id = False):
        """
        keep_id is used for the push methods.
        """
        table_col = BASEBDD._table_keys(table)
        if columns is None:
            columns = table_col
            to_remove = []
            for col in columns:
                if not keep_id and (col == 'id' or col.endswith("_id")):
                    to_remove.append(col)
            for rm in to_remove:
                columns.remove(rm)
        elif not isinstance(columns, list):
            columns = [columns]
        for col in columns:
            if col in table_col:
                select.append(table.c[col].label(col))

    @staticmethod
    def _table_keys(table):
        return table.columns.keys()


class TABLE:
    """
    Class containing the tables of a bdd
    """
    pass
