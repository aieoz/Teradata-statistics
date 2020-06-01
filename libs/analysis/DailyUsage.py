import pandas as pd
import libs.analysis.libs.AbstractAnalysis

class DailyUsage(libs.analysis.libs.AbstractAnalysis.AbstractAnalysis):
    system_table = "daily_usage"
    fill_table = "collect_daily_usage"
    v_create_table = None
    v_table_name = None

    def __init__(self, settings):
        super().__init__(settings, self)
    
    def read(self, table_names, begin, end):
        # Export data to JSON
        result = {}
        result["operation"] = "Daily usage"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/read/daily_usage.sql', mode="r")
        SQL = file.read()
        file.close()

        for table_name in table_names:
            settings = {
                "DATABASE_NAME": table_name.split(".")[0],
                "TABLE_NAME": table_name.split(".")[1],
                "BEGIN": begin,
                "END": end
            }
            sSQL = self.replace_sql(SQL, settings)

            db_name = table_name.split(".")[0]
            tb_name = table_name.split(".")[1]

            table_results = {
                "table_name": tb_name,
                "database_name": db_name,
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
