class IngestionValidationError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class SemanticDuplicateError(Exception):
    def __init__(self, message: str, existing_external_id: str | None = None) -> None:
        self.message = message
        self.existing_external_id = existing_external_id
        super().__init__(message)
