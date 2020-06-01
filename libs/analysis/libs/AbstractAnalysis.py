import pandas as pd

class AbstractAnalysis:
    
    def __init__(self, settings, child):
        self.connection = None
        self.settings = settings
        self.system_table = child.system_table
        # Main processing macro
        self.fill_table = child.fill_table
        # Creating volatile table macro
        self.v_create_table = child.v_create_table
        self.v_table_name = child.v_table_name

    def set_connection(self, connection):
        self.connection = connection

        # Create, specific for analysis, volatile table
        if self.v_create_table:
            self.connection.execute("EXEC TDSP_ANALYSIS." + self.v_create_table + ";")
    
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
            "SYSTEM_TABLE_NAME": self.system_table,
            "TABLE_NAME": tb_name,
            "DATABASE_NAME": db_name
        }

        SQL = self.replace_sql(SQL, settings)

        days = []

        for day in pd.read_sql(SQL, self.connection).values:
            days.append(day[0])

        return days

    def update(self, table, date):
        self.test_connection()

        db_name = table.split(".")[0]
        tb_name = table.split(".")[1]
        macro = self.fill_table

        SQL = f"EXEC TDSP_ANALYSIS.{macro}('{db_name}', '{tb_name}', '{date}');"

        self.connection.execute(SQL)

        if self.v_create_table:
            self.connection.execute("DELETE FROM " + self.v_table_name + ";")

