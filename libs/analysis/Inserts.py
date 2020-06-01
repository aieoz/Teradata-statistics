import pandas as pd
import libs.analysis.libs.AbstractAnalysis

class Inserts(libs.analysis.libs.AbstractAnalysis.AbstractAnalysis):
    system_table = "traffic_type"
    fill_table = "collect_traffic_type"
    v_create_table = "collect_traffic_type_create_volatile"
    v_table_name = "v_traffic_type"

    def __init__(self, settings):
        super().__init__(settings, self)
    
    def read(self, table_names, begin, end):
        # Export data to JSON
        result = {}
        result["operation"] = "Traffic type"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/read/inserts.sql', mode="r")
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
            table_results_collector = {}

            for time_id in pd.read_sql(sSQL, self.connection).values:
                period_begin = time_id[0]
                scope = int(time_id[1])
                inserts = int(time_id[2])

                if not period_begin in table_results_collector:
                    table_results_collector[period_begin] = {}
                
                if not scope in table_results_collector[period_begin]:
                    table_results_collector[period_begin][scope] = {}

                table_results_collector[period_begin][scope] = {
                    "date": period_begin,
                    "inserts": inserts,
                }

            for collected in table_results_collector:
                min_inserts = table_results_collector[collected][1]["inserts"]
                max_inserts = min_inserts + table_results_collector[collected][0]["inserts"]

                table_results["periods"].append({
                    "date": str(table_results_collector[collected][0]["date"]),
                    "statements": [
                            {
                            "statement_type": "insert",
                            "type": "min",
                            "value": min_inserts
                            },
                            {
                            "statement_type": "insert",
                            "type": "max",
                            "value": max_inserts
                            }
                    ]
                })

            
            result["tables"].append(table_results)
        
        return result
