"""Persist optional authoring features independently of metadata snapshot ordering."""

import json

EXTENSION_KEYS = ("evaluation", "grouped_outputs")


def read_extensions(adapter):
    if not adapter.table_exists("idr_meta.config_extension"):
        return {}
    result = {}
    for row in adapter.query("SELECT extension_key, value_json FROM idr_meta.config_extension"):
        if row["extension_key"] in EXTENSION_KEYS:
            result[row["extension_key"]] = json.loads(row["value_json"])
    return result
