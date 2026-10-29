"""serverless-chaos: chaos engineering for event-driven serverless apps.

NOTE: this __init__ must stay import-free. The `runtime` and `adapters` subpackages are
shipped inside Lambda deployment packages and may only depend on boto3 + the stdlib.
"""
__version__ = "0.1.0"
