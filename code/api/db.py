from databricks.connect import DatabricksSession

_spark = None

def get_spark():
    """Return (or lazily create) the shared DatabricksSession."""
    global _spark
    if _spark is None:
        _spark = DatabricksSession.builder.getOrCreate()
    return _spark
