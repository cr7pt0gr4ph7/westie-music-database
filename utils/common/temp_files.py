import os
from typing import Callable, Concatenate, LiteralString, ParamSpec, overload


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
            try:
                os.remove(file_name)
            except FileNotFoundError as e:
                print(f"Ignoring {e}")

        self.temp_files.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None and not self.delete_on_error:
            pass

        self.delete_now()

@overload
def with_temp_files[**P, R](f: Callable[Concatenate[TempFileTracker, P], R]):
    pass

@overload
def with_temp_files[**P, R](f: None = None, /, *, delete_on_error: bool = True) -> Callable[[Callable[Concatenate[TempFileTracker, P], R]], Callable[P, R]]:
    pass

def with_temp_files[**P, R](f: None | Callable[Concatenate[TempFileTracker, P], R] = None, /, *, delete_on_error: bool = True):
    def wrap(f: Callable[Concatenate[TempFileTracker, P], R]):
        def inner(*args: P.args, **kwargs: P.kwargs) -> R:
            with TempFileTracker(delete_on_error=delete_on_error) as temp_files:
                return f(temp_files, *args, **kwargs)

        return inner

    # See if we're being called as @with_temp_files or @with_temp_files().
    if f is None:
        # We're called with parens.
        return wrap

    # We're called as @dataclass without parens.
    return wrap(f)
