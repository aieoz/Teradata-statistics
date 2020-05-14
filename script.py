import libs.utils
import argparse
import json
import getpass

password = getpass.getpass()
settings = libs.utils.read_settings()
settings["password"] = password
args = libs.utils.set_args(settings)

# Read settings
runtime_settings = {}
runtime_settings["enabled_analysis"]    = {}
runtime_settings["all_analysis"]        = {}
runtime_settings["analysis"]            = {}
source_to_target                        = {}


libs.utils.get_begin_end(args, runtime_settings, settings)


for an in settings["analysis"]:
    source_to_target[an["short"]] = an["target"]

    if an["enabled"]:
        runtime_settings["enabled_analysis"][an["short"]] = []

    runtime_settings["all_analysis"][an["short"]] = []
    runtime_settings["all_analysis"][an["short"]] = an.get("default_tables", [])

# Analysis to perform
if args.A:
    for an in args.A:
        if not an in runtime_settings["all_analysis"]:
            argparse.ArgumentParser.exit(-1, "Nieznana analiza: " + an)
        runtime_settings["analysis"][an] = []
else:
    runtime_settings["analysis"] = runtime_settings["enabled_analysis"]


# Tables to process
if args.T:
    runtime_settings["tables"] = args.T

    for an in runtime_settings["analysis"]:
        runtime_settings["analysis"][an] = args.T
else:

    for an in runtime_settings["analysis"]:
        runtime_settings["analysis"][an] = runtime_settings["all_analysis"][an]

print("Początek czasu analizy:  ", runtime_settings["begin"])
print("Koniec czasu analizy:    ", runtime_settings["end"])
print("Analizy:                 ")
for an in runtime_settings["analysis"]:
    print(an, ' na tabelach ', ', '.join(runtime_settings["analysis"][an]))


print("\n\nUpdating missing analysis: ")

# Create database connection
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
                new_one.update(table_name, day)


        # Read results
        results = new_one.read(runtime_settings["analysis"][an], runtime_settings["begin"], runtime_settings["end"])

        with open(source_to_target[an], 'w') as file:
            json.dump(results, file, indent = 4)
        

# print(runtime_settings)
