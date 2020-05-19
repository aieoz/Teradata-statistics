import pandas as pd
import libs.analysis.AbstractAnalysis

class PreCreator:
    orders = {}
    initialized = False
    
    @staticmethod
    def fill(connection, date, database_name, table_name, sql_file):
        if not PreCreator.initialized:
            PreCreator.init(connection)

        file = open(sql_file, mode="r")
        SQL = file.read()
        file.close()

        settings = {
            "DAY": date,
            "DATABASE_NAME": database_name,
            "TABLE_NAME": table_name
        }

        
        SQL = libs.analysis.AbstractAnalysis.AbstractAnalysis.replace_sql(SQL, settings)
        connection.execute(SQL)

    @staticmethod
    def init(connection):
        file = open('sqls/volatile_create_1.sql', mode="r")
        SQL = file.read()
        file.close()
        connection.execute(SQL)

        file = open('sqls/volatile_create_2.sql', mode="r")
        SQL = file.read()
        file.close()
        connection.execute(SQL)

        PreCreator.initialized = True


    