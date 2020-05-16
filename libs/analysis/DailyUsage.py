import pandas as pd
import libs.analysis.AbstractAnalysis

class DailyUsage(libs.analysis.AbstractAnalysis.AbstractAnalysis):
    system_table = "daily_usage"

    def __init__(self, settings):
        super().__init__(settings, self.system_table)

    def update(self, table, date):
        self.test_connection()

        file = open('sqls/daily_usage/daily_usage.sql', mode="r")
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

        file = open('sqls/daily_usage/daily_usage_read.sql', mode="r")
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

            table_results = {
                "table_name": table_name,
                "periods": []
            }
            for hour_id in pd.read_sql(sSQL, self.connection).values:
                hour = int(hour_id[0])
                sum_uses = hour_id[1]
                avg_uses = hour_id[2]
                max_uses = hour_id[3]

                table_results["periods"].append({
                    "period_begin": str(hour) + ":00",
                    "period_end": str(hour + 1) + ":00",
                    "uses_total": sum_uses,
                    "uses_average": avg_uses,
                    "uses_max": max_uses
                })
            
            result["tables"].append(table_results)
        
        return result
