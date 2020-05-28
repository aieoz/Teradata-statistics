import datetime
from datetime import timedelta
import random

tables = [
    {
        "database": "movies",
        "table": "movies",
        "inserts": 10,
        "du": 24
    },
    {
        "database": "movies",
        "table": "actors",
        "inserts": 9,
        "du": 20
    },
    {
        "database": "movies",
        "table": "directors",
        "inserts": 10,
        "du": 10,
    },
    {
        "database": "movies",
        "table": "directors_genres",
        "inserts": 6,
        "du": 11   
    },
    {
        "database": "movies",
        "table": "movies_directors",
        "inserts": 8,
        "du": 10
    },
    {
        "database": "movies",
        "table": "movies_genres",
        "inserts": 4,
        "du": 8
    },
    {
        "database": "movies",
        "table": "roles",
        "inserts": 19,
        "du": 8
    },
    {
        "database": "cinemas",
        "table": "buildings",
        "inserts": 2,
        "du": 6
    },
    {
        "database": "cinemas",
        "table": "owners",
        "inserts": 3,
        "du": 1
    },
    {
        "database": "cinemas",
        "table": "premieres",
        "inserts": 22,
        "du": 12
    },
    {
        "database": "cinemas",
        "table": "events",
        "inserts": 23,
        "du": 19
    },
    {
        "database": "cinemas",
        "table": "sales",
        "inserts": 6,
        "du": 11
    },
    {
        "database": "cinemas",
        "table": "clients",
        "inserts": 1,
        "du": 217
    }
]

hours = [
    {
        "id": 0,
        "mp": 14
    },
    {
        "id": 1,
        "mp": 6
    },
    {
        "id": 2,
        "mp": 2
    },
    {
        "id": 3,
        "mp": 1
    },
    {
        "id": 4,
        "mp": 1
    },
    {
        "id": 5,
        "mp": 1
    },
    {
        "id": 6,
        "mp": 2
    },
    {
        "id": 7,
        "mp": 4
    },
    {
        "id": 8,
        "mp": 16
    },
    {
        "id": 9,
        "mp": 17
    },
    {
        "id": 10,
        "mp": 18
    },
    {
        "id": 11,
        "mp": 17
    },
    {
        "id": 12,
        "mp": 19
    },
    {
        "id": 13,
        "mp": 19
    },
    {
        "id": 14,
        "mp": 20
    },
    {
        "id": 15,
        "mp": 16
    },
    {
        "id": 16,
        "mp": 10
    },
    {
        "id": 17,
        "mp": 8
    },
    {
        "id": 18,
        "mp": 3
    },
    {
        "id": 19,
        "mp": 1
    },
    {
        "id": 20,
        "mp": 1
    },
    {
        "id": 21,
        "mp": 1
    },
    {
        "id": 22,
        "mp": 1
    },
    {
        "id": 23,
        "mp": 3
    }
]

users = [
    {
        "name": "dbc",
        "id": 'FFFE'
    },
    {
        "name": "kamil",
        "id": 'FFFD'
    },
    {
        "name": "system",
        "id": 'FFFC'
    },
    {
        "name": "reader",
        "id": 'FFFB'
    },
    {
        "name": "controller",
        "id": 'FFFA'
    },
    {
        "name": "sys1",
        "id": 'FFAF'
    },
    {
        "name": "sys2",
        "id": 'FFBF'
    },
    {
        "name": "sys3",
        "id": 'FFCF'
    },
    {
        "name": "sys4",
        "id": 'FFDF'
    },
    {
        "name": "sys5",
        "id": 'FFEF'
    },
    {
        "name": "sys6",
        "id": 'FFFF'
    },
    {
        "name": "sys7",
        "id": 'FAFF'
    },
    {
        "name": "sys8",
        "id": 'FBFF'
    },
    {
        "name": "sys9",
        "id": 'FCFF'
    },
]

ins_base        = 186
du_base         = 23
tu_base         = 2240
change_ratio    = 0.2
weekend         = 0.1
group_multi     = 1.1


###########################
######### INSERTS #########
###########################
for table in tables:
    table_name      = table["table"]
    database_name   = table["database"]

    start           = datetime.datetime(2019, 11, 1)
    end             = datetime.datetime(2019, 12, 31)

    while start <= end:
        datestring = start.strftime("%Y-%m-%d")

        action_base     = ins_base * table["inserts"]
        inserts         = (int) (action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio)))
        group_base      = (int) (inserts + (random.randint(0, (int) (inserts * group_multi)) - (group_multi / 2)))
        inserts_group   = max(inserts, group_base)

        if start.weekday() == 5 or start.weekday() == 6:
            inserts = (int) (inserts * weekend)
            inserts_group = (int) (inserts_group * weekend)
        
        if inserts_group < inserts:
            raise Exception("Something is wrong here ://")

        SQL     = "INSERT INTO TDSP_ANALYSIS.inserts (measure_date, table_name, database_name, complete, insert_single, insert_group) VALUES "
        SQL    += f"('{datestring}', '{table_name}', '{database_name}', 1, {inserts}, {inserts_group});"
        print(SQL)

        start = start + timedelta(days=1)

###########################
#########  DAILY  #########
###########################
for table in tables:
    table_name      = table["table"]
    database_name   = table["database"]

    start           = datetime.datetime(2019, 11, 1)
    end             = datetime.datetime(2019, 12, 31)

    while start <= end:
        datestring = start.strftime("%Y-%m-%d")

        for hour in hours:
            houd_id = hour["id"]
            

            action_base     = du_base * table["du"]
            uses_total      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * hour["mp"])

            SQL     = "INSERT INTO TDSP_ANALYSIS.daily_usage (measure_date, measure_hour, table_name, database_name, complete, uses_total) VALUES "
            SQL    += f"('{datestring}', '{houd_id}', '{table_name}', '{database_name}', 1, {uses_total});"
            print(SQL)

        start = start + timedelta(days=1)

#################################
#########  table_usage  #########
#################################
for table in tables:
    table_name      = table["table"]
    database_name   = table["database"]

    start           = datetime.datetime(2019, 11, 1)
    end             = datetime.datetime(2019, 12, 31)

    while start <= end:
        datestring = start.strftime("%Y-%m-%d")

        action_base     = tu_base * table["inserts"]
        uses_total      = (int) (action_base - (action_base * change_ratio / 2) + random.randint(0, (action_base * change_ratio)))

        if start.weekday() == 5 or start.weekday() == 6:
            uses_total = (int) (uses_total * weekend)

        SQL     = "INSERT INTO TDSP_ANALYSIS.table_usage (measure_date, table_name, database_name, complete, uses_total) VALUES "
        SQL    += f"('{datestring}', '{table_name}', '{database_name}', 1, {uses_total});"
        print(SQL)

        start = start + timedelta(days=1)

##################################
#########  traffic_type  #########
##################################
for table in tables:
    table_name      = table["table"]
    database_name   = table["database"]

    start           = datetime.datetime(2019, 11, 1)
    end             = datetime.datetime(2019, 12, 31)

    while start <= end:
        selects = 0
        inserts = 0
        updates = 0
        deletes = 0
        inssels = 0

        for scope in [0,1]:
            datestring = start.strftime("%Y-%m-%d")

            action_base     = tu_base * table["inserts"]
            selects      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * 3)
            inserts      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * 1)
            updates      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * 0.01)
            deletes      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * 0.01)
            inssels      = (int) ((action_base - (action_base * change_ratio / 2) + random.randint(0, (int) (action_base * change_ratio))) * 0.1)

            if start.weekday() == 5 or start.weekday() == 6:
                inserts = (int) (inserts * weekend)
                selects = (int) (selects * weekend)
                updates = (int) (updates * weekend)
                deletes = (int) (deletes * weekend)
                inssels = (int) (inssels * weekend)

            SQL     = "INSERT INTO TDSP_ANALYSIS.traffic_type (measure_date, table_name, database_name, complete, scope, SelectOption, InsertOption, UpdateOption, DeleteOption, InsSelOption) VALUES "
            SQL    += f"('{datestring}', '{table_name}', '{database_name}', 1, {scope}, {selects}, {inserts}, {updates}, {deletes}, {inssels});"
            print(SQL)

        start = start + timedelta(days=1)

#######################################
#########  traffic_type_user  #########
#######################################
for table in tables:
    table_name      = table["table"]
    database_name   = table["database"]

    start           = datetime.datetime(2019, 11, 1)
    end             = datetime.datetime(2019, 12, 31)

    while start <= end:
        datestring = start.strftime("%Y-%m-%d")
        
        for statement in ["Select", "Insert"]:

            values = []
            for z in range(10):
                values.append(random.randint(0, 20000))

            values.sort()
            random.shuffle(users)

            for k in range(10):
                usr_id = users[k]["id"]
                usr_name = users[k]["name"]



                SQL     = "INSERT INTO TDSP_ANALYSIS.traffic_type_user (measure_date, table_name, database_name, complete, statement_type, num_of, user_id, user_name, total) VALUES "
                SQL    += f"('{datestring}', '{table_name}', '{database_name}', 1, '{statement}', {k}, '{usr_id}'xb, '{usr_name}', {values[k]});"
                print(SQL)


        start = start + timedelta(days=1)