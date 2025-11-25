from itertools import pairwise
from typing import Any

import ast
import inspect
import textwrap


def get_attr_docstrings(cls: type[Any]) -> dict[str, str]:
    """Get any docstrings placed after attribute assignments in a class body."""
    cls_node = ast.parse(textwrap.dedent(inspect.getsource(cls))).body[0]

    if not isinstance(cls_node, ast.ClassDef):
        raise TypeError("Given object was not a class.")

    out = {}

    # Consider each pair of nodes.
    for a, b in pairwise(cls_node.body):
        # Must be an assignment then a constant string.
        if (
            not isinstance(a, ast.Assign | ast.AnnAssign)
            or not isinstance(b, ast.Expr)
            or not isinstance(b.value, ast.Constant)
            or not isinstance(b.value.value, str)
        ):
            continue

        doc = inspect.cleandoc(b.value.value)

        if isinstance(a, ast.Assign):
            # An assignment can have multiple targets (a = b = v).
            targets = a.targets
        else:
            # An annotated assignment only has one target.
            targets = [a.target]

        for target in targets:
            # Must be assigning to a plain name.
            if not isinstance(target, ast.Name):
                continue

            out[target.id] = doc

    return out
