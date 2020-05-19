import pandas as pd
import libs.analysis.PreCreator

class AbstractAnalysis:
    
    def __init__(self, settings, system_table, fill_table, v_fill_table):
        self.connection = None
        self.settings = settings
        self.system_table = system_table
        self.fill_table = fill_table
        self.v_fill_table = v_fill_table

    def set_connection(self, connection):
        self.connection = connection
    
    def test_connection(self):
        if self.connection == None:
            raise Exception("Database connection unavalible for Daily Usage")
    
    @staticmethod
    def replace_sql(SQL, settings):

        for setting in settings:
            SQL = SQL.replace("$" + setting, settings[setting])
        
        if "$" in SQL:
            raise Exception("Missing parameters for: " + SQL)
        
        return SQL

    def days_missing(self, table_name, begin, end):
        self.test_connection()

        file = open('sqls/missing.sql', mode="r")
        SQL = file.read()
        file.close()

        db_name = table_name.split(".")[0]
        tb_name = table_name.split(".")[1]

        settings = {
            "BEGIN": begin,
            "END": end,
            "SYSTEM_DATABASE_NAME": self.settings["analysis_database"],
            "SYSTEM_TABLE_NAME": self.system_table,
            "TABLE_NAME": tb_name,
            "DATABASE_NAME": db_name
        }

        SQL = self.replace_sql(SQL, settings)
        print(SQL)

        days = []

        for day in pd.read_sql(SQL, self.connection).values:
            days.append(day[0])

        return days

    def update(self, table, date):
        self.test_connection()

        file = open(self.fill_table, mode="r")
        SQL = file.read()
        file.close()

        db_name = table.split(".")[0]
        tb_name = table.split(".")[1]

        settings = {
            "DAY": date,
            "DATABASE_NAME": db_name,
            "SYSTEM_DATABASE_NAME": self.settings["analysis_database"],
            "TABLE_NAME": tb_name
        }

        libs.analysis.PreCreator.PreCreator.fill(self.connection, date, db_name, tb_name, self.v_fill_table)

        SQL = self.replace_sql(SQL, settings)
        self.connection.execute(SQL)
