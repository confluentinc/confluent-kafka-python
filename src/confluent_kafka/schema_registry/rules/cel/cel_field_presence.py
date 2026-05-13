# Copyright 2024 Confluent, Inc.
# Copyright 2023 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
from typing import Any, Optional

import celpy
import lark

_has_state = threading.local()


def in_has() -> bool:
    """
    Returns true if inside of CEL interpreter `has` macro.

    This enables working around an issue in cel-python where it is not possible
    to implement protobuf semantics around the `has` macro.

    https://github.com/cloud-custodian/cel-python/issues/73
    """
    return getattr(_has_state, "in_has", False)


# Method-call macros that the standard CEL evaluator handles directly via
# `member_dot_arg`. We must never intercept these — they're not user-registered
# functions and must keep their stdlib semantics.
_RESERVED_MACROS = frozenset(
    ["map", "filter", "all", "exists", "exists_one", "reduce", "min"])


def _extract_namespace_path(tree: Any) -> Optional[str]:
    """
    If ``tree`` is a pure identifier chain (e.g., ``decimals`` or ``foo.bar``)
    with no function calls / indexing / literals, return the dotted path
    string. Otherwise return None.

    Used by the namespace-aware ``member_dot_arg`` override below to detect
    rule fragments like ``decimals.ge(a, b)`` and look up
    ``"decimals.ge"`` as a flat function-registry key — which is what
    cel-java/go/cpp do for namespaced extensions (decimals.*, variants.*,
    timestamp.*), but which celpy doesn't natively support.
    """
    if not isinstance(tree, lark.Tree):
        return None
    if tree.data == "member_dot":
        # member_dot: member "." IDENT  (a value-position access, no parens)
        # Two children: left (member tree), right (IDENT token).
        left = _extract_namespace_path(tree.children[0])
        if left is None:
            return None
        right = tree.children[1]
        if not isinstance(right, lark.Token) or right.type != "IDENT":
            return None
        return f"{left}.{right.value}"
    if tree.data in ("member", "primary"):
        # Pass-through: single child wrapping the actual node.
        if len(tree.children) != 1:
            return None
        return _extract_namespace_path(tree.children[0])
    if tree.data == "ident":
        # ident: IDENT
        child = tree.children[0]
        if isinstance(child, lark.Token) and child.type == "IDENT":
            return child.value
        return None
    return None


class InterpretedRunner(celpy.InterpretedRunner):
    def evaluate(self, context: Any) -> Any:
        class Evaluator(celpy.Evaluator):
            def macro_has_eval(self, exprlist: Any) -> celpy.celtypes.BoolType:
                _has_state.in_has = True
                result = super().macro_has_eval(exprlist)
                _has_state.in_has = False
                return result

            def member_dot_arg(self, tree: Any) -> Any:
                """
                Adds namespace-aware function dispatch to celpy.

                Standard celpy treats ``x.foo(args)`` as a method call on
                ``x``, looking up bare ``foo`` in the function registry. That
                makes dotted function names like ``decimals.ge(a, b)`` (used
                by the schema-registry CEL extensions, matching cel-java /
                cel-go / cel-cpp) impossible.

                Override: if the receiver is a pure identifier path
                (``decimals``, ``timestamp``, ``foo.bar``) and
                ``{path}.{method}`` exists in the function registry, dispatch
                that flat function directly with the explicit args. Otherwise
                fall through to celpy's standard method dispatch (which keeps
                ``"hi".startsWith("h")``, ``ts.getDate()``, ``list.all(...)``
                etc. working).
                """
                if (
                    isinstance(tree, lark.Tree)
                    and len(tree.children) >= 2
                ):
                    member_tree = tree.children[0]
                    method_token = tree.children[1]
                    if (
                        isinstance(method_token, lark.Token)
                        and method_token.type == "IDENT"
                        and method_token.value not in _RESERVED_MACROS
                    ):
                        path = _extract_namespace_path(member_tree)
                        if path is not None:
                            candidate = f"{path}.{method_token.value}"
                            funcs = getattr(
                                self.activation, "functions", None)
                            if funcs is not None and candidate in funcs:
                                # The dotted-function name is registered —
                                # dispatch it directly. Precedence: a
                                # registered `{path}.{method}` always wins
                                # over standard method-call dispatch. This
                                # lets the namespaced extensions work even
                                # when the path's root identifier is a
                                # stdlib value (e.g., `timestamp` is bound
                                # to celpy's TimestampType class, but
                                # `timestamp.of` is our extension).
                                func = funcs[candidate]
                                if len(tree.children) == 3:
                                    args = list(self.visit(tree.children[2]))
                                else:
                                    args = []
                                return func(*args)
                return super().member_dot_arg(tree)

        e = Evaluator(ast=self.ast, activation=self.new_activation())
        value = e.evaluate(context)
        return value


def _is_bound_variable(activation: Any, name: str) -> bool:
    """Return True if `name` resolves to a bound variable in the activation."""
    try:
        activation.resolve_variable(name)
        return True
    except Exception:
        return False
