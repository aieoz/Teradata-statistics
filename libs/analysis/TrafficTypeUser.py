import pandas as pd
import libs.analysis.AbstractAnalysis
import libs.analysis.PreCreator

class TrafficTypeUser(libs.analysis.AbstractAnalysis.AbstractAnalysis):
    system_table = "traffic_type_user"
    fill_table = "sqls/traffic_type_user/traffic_type_user.sql"
    v_fill_table = "sqls/traffic_type_user/volatile_fill.sql"

    def __init__(self, settings):
        super().__init__(settings, self.system_table, self.fill_table, self.v_fill_table)

    def read(self, table_names, begin, end):
        result = {}
        result["operation"] = "Traffic type with user info"
        result["begin"] = begin
        result["end"] = end
        result["tables"] = []

        file = open('sqls/traffic_type_user/traffic_type_user_read.sql', mode="r")
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
                "statement_types": {}
            }

            for time_id in pd.read_sql(sSQL, self.connection).values:
                statement_type = str(time_id[0])
                user_id     = int.from_bytes(time_id[1], byteorder='big', signed=False)
                user_name   = str(time_id[2])
                uses        = int(time_id[3])

                if statement_type not in table_results["statement_types"]:
                    table_results["statement_types"][statement_type] = {
                        "users": []
                    }

                table_results["statement_types"][statement_type]["users"].append({
                    "user_id": user_id,
                    "user_name": user_name,
                    "uses": uses
                })

            
            result["tables"].append(table_results)
        
        return result