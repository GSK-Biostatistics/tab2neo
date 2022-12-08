import neointerface
import os
import jaydebeapi
import pandas as pd


class HiveDataLoader:
    def __init__(self,
                 db_host=os.environ.get("HIVE_SERVER"),
                 schema=os.environ.get("HIVE_SCHEMA"),
                 user=os.environ.get("HIVE_USER_ID"),
                 password=os.environ.get("HIVE_PASSWORD"),
                 driver_path=os.environ.get("HIVE_JDBC_DRIVER_PATH"),
                 driver_class=os.environ.get("HIVE_JDBC_DRIVER"),
                 *args, **kwargs):
        self._connection = None
        self._schema = schema
        self._driver_class = driver_class
        self._db_host = db_host
        self._user = user
        self._password = password
        self._driver_path = driver_path

    def open(self):
        self._connection = jaydebeapi.connect(self._driver_class, f'jdbc:hive2://{self._db_host}/{self._schema}',
                                              {'UID': self._user, 'PWD': self._password, 'AuthMech': "3"}, self._driver_path)

    def query(self, statement: str):
        cursor = self._connection.cursor()
        cursor.execute(statement)
        results = cursor.fetchall()
        names = [item[0] for item in cursor.description]
        df = pd.DataFrame(results)
        df.columns = names
        cursor.close()
        return df

    def get_tables(self):
        tables_df = self.query(f'show tables from {self._schema}')
        return tables_df

    def close(self):
        self._connection.close()
