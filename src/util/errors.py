class CustomException(Exception):
    """
    A generic exception class for handling custom exceptions in the project.
    """

    def __init__(self, message, code=None, details=None):
        """
        Initialize the exception with a message, optional code, and additional details.

        :param message: The error message.
        :param code: An optional error code (e.g., HTTP status code or custom code).
        :param details: Additional details about the exception (e.g., context or metadata).
        """
        super().__init__(message)
        self.code = code
        self.details = details

    def __str__(self):
        """
        Return a string representation of the exception.
        """
        base_message = f"Error: {self.args[0]}"
        if self.code:
            base_message += f" (Code: {self.code})"
        if self.details:
            base_message += f" | Details: {self.details}"
        return base_message
