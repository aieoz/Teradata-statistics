import pandas as pd
import libs.analysis.AbstractAnalysis
import libs.analysis.PreCreator

class Inserts(libs.analysis.AbstractAnalysis.AbstractAnalysis):
    system_table = "inserts"
    fill_file = "sqls/inserts/inserts.sql"
    v_fill_table = "sqls/inserts/volatile_fill.sql"

    def __init__(self, settings):
        super().__init__(settings, self.system_table, self.fill_file, self.v_fill_table)

    def read(self, table_names, begin, end):
        result = {}
        result["operation"] = "Traffic type with user info"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/inserts/inserts_read.sql', mode="r")
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
            print(sSQL)

            table_results = {
                "table_name": table_name,
                "days": []
            }

            for time_id in pd.read_sql(sSQL, self.connection).values:
                date            = str(time_id[0])
                insert_single   = int(time_id[1])
                insert_group    = int(time_id[2])

                table_results["days"].append({
                    "measure_date": date,
                    "inserts_min": insert_single,
                    "inserts_max": insert_single + insert_group
                })
            
            result["tables"].append(table_results)

        
        return result