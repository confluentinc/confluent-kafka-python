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

import celpy  # type: ignore
from celpy import celtypes  # type: ignore

QUOTE_TRANS = str.maketrans(
    {
        "\a": r"\a",
        "\b": r"\b",
        "\f": r"\f",
        "\n": r"\n",
        "\r": r"\r",
        "\t": r"\t",
        "\v": r"\v",
        "\\": r"\\",
        '"': r"\"",
    }
)


def quote(s: str) -> str:
    return '"' + s.translate(QUOTE_TRANS) + '"'


class StringFormat:
    """An implementation of string.format() in CEL."""

    def __init__(self, locale: str):
        self.locale = locale

    def format(self, fmt: celtypes.Value, args: celtypes.Value) -> celpy.Result:
        if not isinstance(fmt, celtypes.StringType):
            return celpy.native_to_cel(celpy.new_error("format() requires a string as the first argument"))
        if not isinstance(args, celtypes.ListType):
            return celpy.native_to_cel(celpy.new_error("format() requires a list as the second argument"))
        # printf style formatting
        i = 0
        j = 0
        result = ""
        while i < len(fmt):
            if fmt[i] != "%":
                result += fmt[i]
                i += 1
                continue

            if i + 1 < len(fmt) and fmt[i + 1] == "%":
                result += "%"
                i += 2
                continue
            if j >= len(args):
                return celpy.CELEvalError("format() not enough arguments for format string")
            arg = args[j]
            j += 1
            i += 1
            if i >= len(fmt):
                return celpy.CELEvalError("format() incomplete format specifier")
            precision = 6
            if fmt[i] == ".":
                i += 1
                precision = 0
                while i < len(fmt) and fmt[i].isdigit():
                    precision = precision * 10 + int(fmt[i])
                    i += 1
            if i >= len(fmt):
                return celpy.CELEvalError("format() incomplete format specifier")
            if fmt[i] == "f":
                result += self.format_float(arg, precision)
            if fmt[i] == "e":
                result += self.format_exponential(arg, precision)
            elif fmt[i] == "d":
                result += self.format_int(arg)
            elif fmt[i] == "s":
                result += self.format_string(arg)
            elif fmt[i] == "x":
                result += self.format_hex(arg)
            elif fmt[i] == "X":
                result += self.format_hex(arg).upper()
            elif fmt[i] == "o":
                result += self.format_oct(arg)
            elif fmt[i] == "b":
                result += self.format_bin(arg)
            else:
                return celpy.CELEvalError("format() unknown format specifier: " + fmt[i])
            i += 1
        if j < len(args):
            return celpy.CELEvalError("format() too many arguments for format string")
        return celtypes.StringType(result)

    def format_float(self, arg: celtypes.Value, precision: int) -> celpy.Result:
        if isinstance(arg, celtypes.DoubleType):
            return celtypes.StringType(f"{arg:.{precision}f}")
        return self.format_int(arg)

    def format_exponential(self, arg: celtypes.Value, precision: int) -> celpy.Result:
        if isinstance(arg, celtypes.DoubleType):
            return celtypes.StringType(f"{arg:.{precision}e}")
        return self.format_int(arg)

    def format_int(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, celtypes.IntType):
            return celtypes.StringType(arg)
        if isinstance(arg, celtypes.UintType):
            return celtypes.StringType(arg)
        return celpy.CELEvalError("format_int() requires an integer argument")

    def format_hex(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, celtypes.IntType):
            return celtypes.StringType(f"{arg:x}")
        if isinstance(arg, celtypes.UintType):
            return celtypes.StringType(f"{arg:x}")
        if isinstance(arg, celtypes.BytesType):
            return celtypes.StringType(arg.hex())
        if isinstance(arg, celtypes.StringType):
            return celtypes.StringType(arg.encode("utf-8").hex())
        return celpy.CELEvalError("format_hex() requires an integer, string, or binary argument")

    def format_oct(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, celtypes.IntType):
            return celtypes.StringType(f"{arg:o}")
        if isinstance(arg, celtypes.UintType):
            return celtypes.StringType(f"{arg:o}")
        return celpy.CELEvalError("format_oct() requires an integer argument")

    def format_bin(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, celtypes.IntType):
            return celtypes.StringType(f"{arg:b}")
        if isinstance(arg, celtypes.UintType):
            return celtypes.StringType(f"{arg:b}")
        if isinstance(arg, celtypes.BoolType):
            return celtypes.StringType(f"{arg:b}")
        return celpy.CELEvalError("format_bin() requires an integer argument")

    def format_string(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, celtypes.StringType):
            return arg
        if isinstance(arg, celtypes.BytesType):
            return celtypes.StringType(arg.hex())
        if isinstance(arg, celtypes.ListType):
            return self.format_list(arg)
        return celtypes.StringType(arg)

    def format_value(self, arg: celtypes.Value) -> celpy.Result:
        if isinstance(arg, (celtypes.StringType, str)):
            return celtypes.StringType(quote(arg))
        if isinstance(arg, celtypes.UintType):
            return celtypes.StringType(arg)
        return self.format_string(arg)

    def format_list(self, arg: celtypes.ListType) -> celpy.Result:
        result = "["
        for i in range(len(arg)):
            if i > 0:
                result += ", "
            result += self.format_value(arg[i])
        result += "]"
        return celtypes.StringType(result)


_default_format = StringFormat("en_US")
format = _default_format.format  # noqa: A001
format_value = _default_format.format_value
