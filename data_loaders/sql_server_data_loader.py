import neointerface
import os
import pandas as pd

class SQLServerDataLoader(neointerface.NeoInterface):
    def __init__(self,
                 sqldburl = os.environ.get("SQL_SERVER"),
                 sqldbname = os.environ.get("SQL_DATABASE"),
                 sqluser = os.environ.get("SQL_USER_ID"),
                 sqlpassword = os.environ.get("SQL_PASSWORD"),
                 *args, **kwargs):
        self.sqldbname = sqldbname
        self.sqluser = sqluser
        self._sqlpassword = sqlpassword
        self._con = f"jdbc:sqlserver://{sqldburl};databaseName={self.sqldbname};user={self.sqluser};password={self._sqlpassword};"
        super().__init__(*args, **kwargs)
        if self.autoconnect:
            self.query("call db.clearQueryCaches()")

    def sql_query(self, sqlq:str):
        q = "CALL apoc.load.jdbc($con, $sqlq) YIELD row RETURN row"
        params = {
            "sqlq": sqlq,
            "con": self._con
        }
        return pd.DataFrame([x['row'] for x in self.query(q, params)])

    def sql_get_tables(self):
        tables_df = self.sql_query("SELECT * FROM information_schema.tables;")
        return tables_df