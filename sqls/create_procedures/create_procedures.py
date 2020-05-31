import sys
sys.path.append("../..")

import libs.utils
import argparse
import json
import os
import getpass
import pandas as pd

# Environmental variables for teradata DBC driver
os.environ['ODBCINST'] = "/opt/teradata/client/ODBC_64/odbcinst.ini"
os.environ['ODBCINI'] = "/opt/teradata/client/ODBC_64/odbc.ini"
os.environ['LD_LIBRARY_PATH'] = "/opt/teradata/client/16.20/lib:/opt/teradata/client/16.20/lib64:" + os.environ.get('LD_LIBRARY_PATH', '')

with libs.utils.create_session("dbc", "dbc", "192.168.0.103") as session:

    # file = open('volatile.sql', mode="r")
    # SQL = file.read()
    # file.close()
    # session.execute(SQL)

    # file = open('collect_daily.sql', mode="r")
    # SQL = file.read()
    # file.close()
    # session.execute(SQL)

    # file = open('collect_inserts.sql', mode="r")
    # SQL = file.read()
    # file.close()
    # session.execute(SQL)

    try:
        session.execute("EXEC TDSP_ANALYSIS.collect_inserts_create_volatile;")
        session.execute("EXEC TDSP_ANALYSIS.collect_tables_usage('movies', 'movies', '2020-05-17');")
    finally:
        session.execute("EXEC TDSP_ANALYSIS.collect_inserts_drop_volatile;")

    # ('movies', 'movies', '2020-05-17');
    # DROP PROCEDURE TDSP_ANALYSIS.collect_inserts;
    # DROP PROCEDURE TDSP_ANALYSIS.collect_daily_usage;