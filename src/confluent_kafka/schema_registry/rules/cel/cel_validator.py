# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
from threading import Lock
from typing import Any, Optional

import celpy
from celpy import celtypes
from google.protobuf import descriptor, message

from confluent_kafka.schema_registry.rules.cel.cel_executor import _value_to_cel
from confluent_kafka.schema_registry.rules.cel.cel_field_presence import InterpretedRunner
from confluent_kafka.schema_registry.rules.cel.constraints import _field_value_to_cel, _msg_to_cel
from confluent_kafka.schema_registry.rules.cel.extra_func import EXTRA_FUNCS
from confluent_kafka.schema_registry.serde import RuleError, ValidationRule, ValidationRuleExecutor


class CelValidator(ValidationRuleExecutor):
    """
    Validation-rule executor backed by CEL. Each rule expression is evaluated with
    ``this`` bound to the value being validated and ``now`` bound to the current
    time, and must return either a bool (False = failed) or a string (non-empty =
    failed, with that string as the failure message).

    Each instance owns its own compiled-program cache, keyed by expression alone —
    unlike the JVM client, celpy programs carry no static type declarations, so the
    same program is reusable across every value shape.
    """

    def __init__(self):
        self._env = celpy.Environment(runner_class=InterpretedRunner)
        self._funcs = EXTRA_FUNCS
        self._cache = _CelProgramCache()

    def execute(self, rule: ValidationRule, schema: Any, message: Any) -> Any:
        name = rule.name if rule.name else "unnamed"
        if message is None:
            # Walkers are expected to enforce skip-on-null before invoking the executor;
            # a None here means a non-compliant caller. Surface the contract violation
            # explicitly rather than trip a confusing CEL evaluation error.
            raise RuleError(
                f"Validation rule '{name}' received a null value; walkers must enforce "
                f"skip-on-null before invoking the executor."
            )
        if not rule.expr:
            raise RuleError(f"Validation rule '{name}' has no expression")

        try:
            this = _to_cel(schema, message)
        except Exception as e:
            raise RuleError(f"Could not convert value for validation rule '{name}'") from e

        try:
            prog = self._program(rule.expr)
        except celpy.CELParseError as e:
            raise RuleError(f"Could not compile validation rule '{name}'") from e

        args = {
            "this": this,
            "now": celtypes.TimestampType(datetime.datetime.now(datetime.timezone.utc)),
        }
        try:
            result = prog.evaluate(args)
        except celpy.CELEvalError as e:
            detail = f" ({rule.doc})" if rule.doc else ""
            raise RuleError(f"Could not execute validation rule '{name}'{detail}") from e

        # BoolType/StringType subclass int/str respectively, so check the CEL types
        # explicitly — an IntType result must not be mistaken for a bool.
        if isinstance(result, (bool, celtypes.BoolType)):
            return bool(result)
        if isinstance(result, (str, celtypes.StringType)):
            return str(result)
        raise RuleError(
            f"Validation rule '{name}' must return bool or string; got {type(result).__name__}"
        )

    def _program(self, expr: str) -> celpy.Runner:
        prog = self._cache.get_program(expr)
        if prog is None:
            ast = self._env.compile(expr)
            prog = self._env.program(ast, functions=self._funcs)
            self._cache.set(expr, prog)
        return prog


class _CelProgramCache(object):
    def __init__(self):
        self.lock = Lock()
        self.programs = {}

    def set(self, expr: str, prog: celpy.Runner):
        with self.lock:
            self.programs[expr] = prog

    def get_program(self, expr: str) -> Optional[celpy.Runner]:
        with self.lock:
            return self.programs.get(expr, None)

    def clear(self):
        with self.lock:
            self.programs.clear()


def _to_cel(schema: Any, value: Any) -> Any:
    """
    Convert a value to its CEL representation. ``schema`` is a hint supplied by the
    walker: a protobuf FieldDescriptor for protobuf field values (needed to convert
    scalars, repeated fields and maps faithfully), and the format's schema object
    otherwise (unused — Avro/JSON values are converted structurally).
    """
    if isinstance(value, message.Message):
        return _msg_to_cel(value)
    if isinstance(schema, descriptor.FieldDescriptor):
        return _field_value_to_cel(value, schema)
    return _value_to_cel(value)
