"""Exceptions the integration tests' steps raise.

Defined at module level in an importable module, as an SDK user's are: a
resumed run finds a recorded exception's class only among loaded modules.
"""


class PaymentDeclined(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
