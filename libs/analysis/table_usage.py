import pandas as pd
import libs.analysis.abstract_analysis

class TableUsage(libs.analysis.abstract_analysis.AbstractAnalysis):
    system_table = "tables_usage"

    def __init__(self, settings):
        super().__init__(settings, self.system_table)
    
    def update(self, table, date):
        self.test_connection()

        file = open('sqls/table_usage/table_usage.sql', mode="r")
        SQL = file.read()
        file.close()

        settings = {
            "DAY": date,
            "DATABASE_NAME": table.split(".")[0],
            "SYSTEM_DATABASE_NAME": self.settings["analysis_database"],
            "TABLE_NAME": table.split(".")[1]
        }

        SQL = self.replace_sql(SQL, settings)
        self.connection.execute(SQL)
    
    def read(self, table_names, begin, end):
        result = {}
        result["operation"] = "Daily usage"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/table_usage/table_usage_read.sql', mode="r")
        SQL = file.read()
        file.close()

        for table_name in table_names:
            settings = {
                "SYSTEM_DATABASE_NAME": self.settings["analysis_database"],
                "DATABASE_NAME": table_name.split(".")[0],
                "TABLE_NAME": table_name.split(".")[0],
                "BEGIN": begin,
                "END": end
            }
            sSQL = self.replace_sql(SQL, settings)
            # print(sSQL)

            table_results = {
                "table_name": table_name,
                "periods": []
            }
            for time_id in pd.read_sql(sSQL, self.connection).values:
                period_begin = time_id[0]
                period_end = time_id[1]
                total_uses = int(time_id[2])

                table_results["periods"].append({
                    "period_begin": period_begin,
                    "period_end": period_end,
                    "uses": total_uses
                })
            
            result["tables"].append(table_results)
        
        return result
