"""Static facts about fault types, shared by the runtime (plain dicts) and the control side (pydantic)."""

# which adapter kind each fault type acts on, and which key of the fault names its target
KIND_OF_TYPE = {
    "LambdaLatency": "compute",
    "LambdaError": "compute",
    "DynamoDBFault": "storage",
    "QueueFault": "queue",
}
TARGET_KEY = {
    "LambdaLatency": "function",
    "LambdaError": "function",
    "DynamoDBFault": "table",
    "QueueFault": "queue",
}
# ServiceOutage names a whole service instead of one resource
KIND_OF_SERVICE = {"dynamodb": "storage", "sqs": "queue", "lambda": "compute"}
