from botocore.exceptions import ClientError


class InjectedFaultError(Exception):
    """Raised inside a Lambda handler by an injected LambdaError fault."""

    def __init__(self, code: str, message: str):
        super().__init__(f"[injected:{code}] {message}")
        self.code = code


def make_client_error(code: str, message: str, operation: str) -> ClientError:
    """Build the exception a real AWS call would raise, so application error handling is exercised realistically."""
    status = {"ProvisionedThroughputExceededException": 400, "TooManyRequestsException": 429}.get(code, 503)
    return ClientError(
        {"Error": {"Code": code, "Message": f"{message} (injected)"},
         "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )
