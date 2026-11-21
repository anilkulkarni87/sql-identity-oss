"""Actionable engine failures whose messages contain no warehouse data."""


class EngineSafeguardError(RuntimeError):
    code = "ENGINE_SAFEGUARD"


class GraphNotConverged(EngineSafeguardError):
    code = "GRAPH_NOT_CONVERGED"


class FullRebuildRequired(EngineSafeguardError):
    code = "FULL_REBUILD_REQUIRED"
