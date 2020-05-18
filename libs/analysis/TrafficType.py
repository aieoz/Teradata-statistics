import pandas as pd
import libs.analysis.AbstractAnalysis
import libs.analysis.PreCreator

class TrafficType(libs.analysis.AbstractAnalysis.AbstractAnalysis):
    system_table = "traffic_type"

    def __init__(self, settings):
        super().__init__(settings, self.system_table)
    
    def update(self, table, date):
        self.test_connection()

        file = open('sqls/traffic_type/traffic_type.sql', mode="r")
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

        libs.analysis.PreCreator.PreCreator.fill(self.connection, date, db_name, tb_name)

        SQL = self.replace_sql(SQL, settings)
        self.connection.execute(SQL)
    
    def read(self, table_names, begin, end):
        result = {}
        result["operation"] = "Daily usage"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/traffic_type/traffic_type_read.sql', mode="r")
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
            table_results_collector = {}

            for time_id in pd.read_sql(sSQL, self.connection).values:
                period_begin = time_id[0]
                period_end = time_id[1]
                scope = int(time_id[2])

                selects = int(time_id[3])
                inserts = int(time_id[4])
                updates = int(time_id[5])
                deletes = int(time_id[6])
                inssels = int(time_id[7])

                if not period_begin in table_results_collector:
                    table_results_collector[period_begin] = {}
                
                if not scope in table_results_collector[period_begin]:
                    table_results_collector[period_begin][scope] = {}

                table_results_collector[period_begin][scope] = {
                    "period_begin": period_begin,
                    "period_end": period_end,
                    "scope": scope,
                    "selects": selects,
                    "inserts": inserts,
                    "updates": updates,
                    "deletes": deletes,
                    "inssels": inssels
                }

            for collected in table_results_collector:
                min_selects = table_results_collector[collected][1]["selects"]
                max_selects = min_selects + table_results_collector[collected][0]["selects"]

                min_inserts = table_results_collector[collected][1]["inserts"]
                max_inserts = min_inserts + table_results_collector[collected][0]["inserts"]

                min_updates = table_results_collector[collected][1]["updates"]
                max_updates = min_updates + table_results_collector[collected][0]["updates"]

                min_deletes = table_results_collector[collected][1]["deletes"]
                max_deletes = min_deletes + table_results_collector[collected][0]["deletes"]

                min_inssels = table_results_collector[collected][1]["inssels"]
                max_inssels = min_inssels + table_results_collector[collected][0]["inssels"]

                period_begin = table_results_collector[collected][1]["period_begin"]
                period_end = table_results_collector[collected][1]["period_end"]

                table_results["periods"].append({
                    "period_begin": period_begin,
                    "period_end": period_end,
                    "min_selects": min_selects,
                    "max_selects": max_selects,
                    "min_inserts": min_inserts,
                    "max_inserts": max_inserts,
                    "min_updates": min_updates,
                    "max_updates": max_updates,
                    "min_deletes": min_deletes,
                    "max_deletes": max_deletes,
                    "min_inssels": min_inssels,
                    "max_inssels": max_inssels
                })
            
            result["tables"].append(table_results)
        
        return result
