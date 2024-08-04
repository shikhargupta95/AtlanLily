
import logging

from core.exceptions.exceptions import ValidationFailedException

def not_null(value, on_failure="return_error"):
    if not value:
        if on_failure == "log_error":
            logging.error(ValidationFailedException("Validation failed."))
        elif on_failure == "raise_error":
            raise ValidationFailedException("Validation failed.")
        else:
            return ValidationFailedException("Validation Failed")
