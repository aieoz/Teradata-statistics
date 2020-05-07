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


libs.utils.get_begin_end(args, runtime_settings, settings)



for an in settings["analysis"]:
    if an["enabled"]:
        runtime_settings["enabled_analysis"][an["short"]] = []

    runtime_settings["all_analysis"][an["short"]] = []
    runtime_settings["all_analysis"][an["short"]] = an["default_tables"]

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


# print(runtime_settings)
