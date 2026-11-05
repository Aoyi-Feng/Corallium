"""ApiError carries an HTTP status out of a writer job (PROTOCOL §13.5).

A writer job runs on the store's single writer thread; the handler must not
flatten its failures into 500 — "no such node" is a 404 and the caller decides
what to do next from that code.
"""


class ApiError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
