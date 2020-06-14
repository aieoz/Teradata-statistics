import libs.utils
import argparse
import json
import os
import getpass

settings = libs.utils.read_settings()

# Environmental variables for teradata DBC driver
os.environ['ODBCINST'] = settings["odbc_location"] + "ODBC_64/odbcinst.ini"
os.environ['ODBCINI'] = settings["odbc_location"] + "ODBC_64/odbc.ini"
os.environ['LD_LIBRARY_PATH'] = settings["odbc_location"] + "16.20/lib:" + settings["odbc_location"] + "16.20/lib64:" + os.environ.get('LD_LIBRARY_PATH', '')

# Read password from stdin
password = getpass.getpass()
settings["password"] = password
args = libs.utils.set_args(settings)

# Read settings
runtime_settings = {}
runtime_settings["enabled_analysis"]    = {}
runtime_settings["all_analysis"]        = {}
runtime_settings["analysis"]            = {}
source_to_target                        = {}

# Read analysis period begin and end from program arguments or settings.json
libs.utils.get_begin_end(args, runtime_settings, settings)

# Read list of analysis from settings.json
for an in settings["analysis"]:
    source_to_target[an["short"]] = an["target"]

    if an["enabled"]:
        runtime_settings["enabled_analysis"][an["short"]] = []

    runtime_settings["all_analysis"][an["short"]] = []
    runtime_settings["all_analysis"][an["short"]] = an.get("default_tables", [])

# Create list of analysis to perform
if args.A:
    for an in args.A:
        if not an in runtime_settings["all_analysis"]:
            argparse.ArgumentParser.exit(-1, "Nieznana analiza: " + an)
        runtime_settings["analysis"][an] = []
else:
    runtime_settings["analysis"] = runtime_settings["enabled_analysis"]


# Create list of tables to process
if args.T:
    runtime_settings["tables"] = args.T

    for an in runtime_settings["analysis"]:
        runtime_settings["analysis"][an] = args.T
else:

    for an in runtime_settings["analysis"]:
        runtime_settings["analysis"][an] = runtime_settings["all_analysis"][an]

# Print analysis details
print("Poczatek czasu analizy:  ", runtime_settings["begin"])
print("Koniec czasu analizy:    ", runtime_settings["end"])
print("Analizy:                 ")
for an in runtime_settings["analysis"]:
    print(an, ' na tabelach ', ', '.join(runtime_settings["analysis"][an]))


print("\n\nUpdating missing analysis: ")

# Init database connection
with libs.utils.create_session(settings["database_user"], settings["password"], settings["database_host"]) as session:

    analysis_objects = {}

    # Each analysis
    for an in runtime_settings["analysis"]:
        new_one = libs.utils.analysis_mapper(an, settings)
        new_one.set_connection(session)
        analysis_objects[an] = {
            "controller": new_one,
            "tables": {}
        }

        # Each table
        for table_name in runtime_settings["analysis"][an]:
            # Check if table exists
            libs.utils.check_table_name(session, table_name)

            days = new_one.days_missing(table_name, runtime_settings["begin"], runtime_settings["end"])

            # Each missing day
            for day in days:
                print(an, ":", table_name, ", ", day)

                # Run update
                new_one.update(table_name, day)


        # Read results
        results = new_one.read(runtime_settings["analysis"][an], runtime_settings["begin"], runtime_settings["end"])

        # Export results to json
        with open(source_to_target[an], 'w') as file:
            json.dump(results, file, indent = 4)

