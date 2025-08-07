"""Custom exceptions and human-readable error representation."""


class CustomException(Exception):
    """A generic exception class for handling custom exceptions.

    Provides better readability and sprint representation for errors and
    includes error codes for internal verifications.

    Attributes:
        message: A string containing the information provided with the error.
        code: (optional) An integer representing the error code.
        details: (optional) A string with extra parameters or metadata.
    """

    def __init__(
        self, message: str, code: int | None = None, details: str | None = None
    ) -> None:
        """Initialize the exception with a message and optional parameters.

        Args:
            message: Basic information related to the error.
            code: An optional error code (e.g., HTTP status or custom code).
            details: Additional info about the exception (e.g., metadata).
        """
        super().__init__(message)
        self.code = code
        self.details = details

    def __str__(self) -> str:
        """Return a string representation of the exception."""
        base_message = f"Error: {self.args[0]}"
        if self.code:
            base_message += f" (Code: {self.code})"
        if self.details:
            base_message += f" | Details: {self.details}"
        return base_message
