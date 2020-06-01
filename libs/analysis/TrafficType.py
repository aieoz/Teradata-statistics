import pandas as pd
import libs.analysis.libs.AbstractAnalysis

class TrafficType(libs.analysis.libs.AbstractAnalysis.AbstractAnalysis):
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

        file = open('sqls/read/traffic_type.sql', mode="r")
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
                    "statements": [
                            {
                            "statement_type": "select",
                            "type": "min",
                            "value": min_selects
                            },
                            {
                            "statement_type": "insert",
                            "type": "min",
                            "value": min_inserts
                            },
                            {
                            "statement_type": "update",
                            "type": "min",
                            "value": min_updates
                            },
                            {
                            "statement_type": "delete",
                            "type": "min",
                            "value": min_deletes
                            },
                            {
                            "statement_type": "inssel",
                            "type": "min",
                            "value": min_inssels
                            },

                            {
                            "statement_type": "select",
                            "type": "max",
                            "value": max_selects
                            },
                            {
                            "statement_type": "insert",
                            "type": "max",
                            "value": max_inserts
                            },
                            {
                            "statement_type": "update",
                            "type": "max",
                            "value": max_updates
                            },
                            {
                            "statement_type": "delete",
                            "type": "max",
                            "value": max_deletes
                            },
                            {
                            "statement_type": "inssel",
                            "type": "max",
                            "value": max_inssels
                            }
                    ]
                })

            
            result["tables"].append(table_results)
        
        return result
