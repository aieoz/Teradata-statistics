import pandas as pd
import libs.analysis.AbstractAnalysis

class TableUsage(libs.analysis.AbstractAnalysis.AbstractAnalysis):
    system_table = "table_usage"
    fill_table = "sqls/table_usage/table_usage.sql"
    v_fill_table = None

    def __init__(self, settings):
        super().__init__(settings, self.system_table, self.fill_table, self.v_fill_table)
    
    def read(self, table_names, begin, end):
        result = {}
        result["operation"] = "Table usage"
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
                "TABLE_NAME": table_name.split(".")[1],
                "BEGIN": begin,
                "END": end
            }
            sSQL = self.replace_sql(SQL, settings)
            # print(sSQL)

            db_name = table_name.split(".")[0]
            tb_name = table_name.split(".")[1]

            table_results = {
                "table_name": tb_name,
                "database_name": db_name,
                "periods": []
            }
            for time_id in pd.read_sql(sSQL, self.connection).values:
                period_begin = time_id[0]
                period_end = time_id[1]
                total_uses = int(time_id[2])

                table_results["periods"].append({
                    "period_begin": period_begin,
                    "period_end": period_end,
                    "total": total_uses
                })
            
            result["tables"].append(table_results)
        
        return result
