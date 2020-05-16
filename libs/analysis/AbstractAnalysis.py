import pandas as pd

class AbstractAnalysis:
    
    def __init__(self, settings, system_table):
        self.connection = None
        self.settings = settings
        self.system_table = system_table

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

        settings = {
            "BEGIN": begin,
            "END": end,
            "SYSTEM_DATABASE_NAME": self.settings["analysis_database"],
            "SYSTEM_TABLE_NAME": self.system_table
        }

        SQL = self.replace_sql(SQL, settings)
        # print(SQL)

        days = []

        for day in pd.read_sql(SQL, self.connection).values:
            days.append(day[0])

        return days

        