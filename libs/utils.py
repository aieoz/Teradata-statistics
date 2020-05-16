import teradata
import argparse
import json
import os
import re
import libs.analysis.TableUsage
import libs.analysis.DailyUsage
import libs.analysis.TrafficType
import pandas as pd

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# Environmental variables for teradata DBC driver
os.environ['ODBCINST'] = "/opt/teradata/client/ODBC_64/odbcinst.ini"
os.environ['ODBCINI'] = "/opt/teradata/client/ODBC_64/odbc.ini"
os.environ['LD_LIBRARY_PATH'] = "/opt/teradata/client/16.20/lib:/opt/teradata/client/16.20/lib64:" + os.environ.get('LD_LIBRARY_PATH', '')


def create_session(username, password, host):
    udaExec = teradata.UdaExec(appName="SQL shell", version="1.0", logConsole=False, odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so")
    connection = udaExec.connect(method="odbc", system=host,username=username,password=password)
    return connection

def print_analysis_description(analysis):
    print("Name:        ", analysis["name"])
    print("Short name:  ", analysis["short"])
    print("Description: ", analysis["description"])
    print("")

def is_date(text):
    pattern = re.compile("^20[0-9][0-9]-([0]{0,1}[1-9]|1[0-2])-([0]{0,1}[1-9]|1[0-9]| 2[0-9]|3[0-1])$")
    return pattern.match(text)

def get_begin_end(args, runtime_settings, settings):
    # Calc begin and end of analysis
    if args.Tp:

        # Validate date format
        for data in args.Tp:
            if not is_date(data):
                argparse.ArgumentParser.exit(-1, "Niepoprawny format daty")
        runtime_settings["begin"] = args.Tp[0]
        runtime_settings["end"] = args.Tp[1]

    elif args.Ts:
        if not is_date(args.Ts[0]):
            argparse.ArgumentParser.exit(-1, "Niepoprawny format daty")
        runtime_settings["begin"] = args.Ts[0]
        runtime_settings["end"] = date.today().strftime("%Y-%m-%d")

    elif args.Tm:
        runtime_settings["end"] = date.today().strftime("%Y-%m-%d")
        runtime_settings["begin"] = (date.today() - relativedelta(months=args.Tm[0])).strftime("%Y-%m-%d")

    elif args.Td:
        runtime_settings["end"] = date.today().strftime("%Y-%m-%d")
        runtime_settings["begin"] = (date.today() - timedelta(days=args.Td[0])).strftime("%Y-%m-%d")

    else:
        runtime_settings["end"] = date.today().strftime("%Y-%m-%d")
        runtime_settings["begin"] = (date.today() - timedelta(days=settings["default_period"])).strftime("%Y-%m-%d")

def set_args(settings):
    # Program arguments
    parser = argparse.ArgumentParser(description='Zestaw analiz wykorzystania danych w systemie Teradata')
    parser.add_argument('-AL', action='store_true', help='Lista dostepnych analiz', required=False)
    parser.add_argument('-A', type=str, nargs="+", help='Lista analiz do wykonania', required=False)
    parser.add_argument('-T', type=str, nargs="+", help='Lista tabel do przeanalizowania', required=False)
    parser.add_argument('-D', type=str, nargs="+", help='Lista baz danych do przeanalizowania', required=False)

    group_time = parser.add_mutually_exclusive_group(required=False)
    group_time.add_argument('-Td', type=int, nargs=1, help='analizowany przedział (w dniach)')
    group_time.add_argument('-Tm', type=int, nargs=1, help='analizowany przedział (w miesiącach)')
    group_time.add_argument('-Ts', nargs=1, type=str, help='Początek analizy w formacie YYYY-MM-DD')
    group_time.add_argument('-Tp', nargs=2, type=str, help='Początek i koniec analizy w formacie YYYY-MM-DD')

    args = parser.parse_args()

    if (args.AL):
        print("")
        for analysis in settings["analysis"]:
            print_analysis_description(analysis)
        exit()

    return args

def read_settings():
    settings = None
    with open('settings.json') as settings_file:
        settings = json.load(settings_file)
    return settings

def analysis_mapper(name, settings):
    if name == "DU":
        return libs.analysis.DailyUsage.DailyUsage(settings)
    elif name == "TU":
        return libs.analysis.TableUsage.TableUsage(settings)
    elif name == "TT":
        return libs.analysis.TrafficType.TrafficType(settings)
    else:
        raise Exception("Unknown analysis " + name)

def check_table_name(connection, table):
    if not "." in table:
        raise Exception("Invalid table name format: " + table)

    database_name = table.split(".")[0]
    table_name = table.split(".")[1]
    
    SQL = "SELECT TableName FROM DBC.TablesV WHERE TableKind = 'T' AND DataBaseName='" + database_name + "'"

    tables = pd.read_sql(SQL,connection)
    tables = [t[0] for t in tables.values]
    
    if table_name not in tables:
        raise Exception("Cannot find table: " + table)