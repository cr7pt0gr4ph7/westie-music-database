import os
from typing import LiteralString, overload


class TempFileTracker(object):
    """Tracks temporary files to delete them afterwards."""

    def __init__(self, *, delete_on_error: bool = True):
        self.delete_on_error = delete_on_error
        self.temp_files: set[str] = set()

    def clear(self):
        """Clear the list of temp files without deleting them."""
        self.temp_files.clear()

    @overload
    def register_for_deletion[Name: LiteralString](self, file_name: Name) -> Name:
        pass

    @overload
    def register_for_deletion(self, file_name: str) -> str:
        pass

    def register_for_deletion(self, file_name: str) -> str:
        if "temp_" not in file_name:
            raise ValueError("Temporary files should have 'temp_` in their name")

        self.temp_files.add(file_name)
        return file_name

    def delete_now(self):
        if len(self.temp_files) == 0:
            return

        print(f"Deleting {len(self.temp_files)} temporary files...")
        for file_name in self.temp_files:
            os.remove(file_name)

        self.temp_files.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None and not self.delete_on_error:
            pass

        self.delete_now()
