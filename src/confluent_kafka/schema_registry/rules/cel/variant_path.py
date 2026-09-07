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

"""The JSONPath subset used by ``variants.path(v, path)`` - a port of Java's
``VariantPath``. Supports:

* ``$`` - root
* ``$.field`` / ``$.field.subfield`` - object field by identifier name
* ``$[i]`` - array element by non-negative integer index
* ``$["quoted key"]`` / ``$['quoted key']`` - quoted key for non-identifier names

Resolution failures (missing field, out-of-bounds index, type mismatch) return ``None``
from :func:`walk`; malformed paths raise :class:`ValueError` at parse time.

Identifier names follow ``[A-Za-z_][A-Za-z0-9_]*``; use the quoted form for any other key.
Negative indices are rejected (no RFC 9535 ``len + i`` semantics).

Quoted-key escapes recognize only ``\\\\`` (a literal backslash) and backslash + the
enclosing quote; any other escape is a parse error rather than being silently decoded
(option B, matching the Java reference). Non-ASCII characters may be written literally;
for keys needing escapes beyond these two, use ``variants.field(v, key)`` with a regular
CEL string.
"""

from functools import lru_cache
from typing import List, Optional, Tuple

from confluent_kafka.schema_registry.confluent.types.variant_utils import Variant, VariantType

# A parsed path is a list of segments. Each segment is a ("field", key) or ("index", idx) pair.
Segment = Tuple[str, object]


def walk(root: Variant, path: str) -> Optional[Variant]:
    """Walk ``root`` following ``path``. Returns the resolved Variant, or ``None`` if any
    segment fails to resolve. Raises :class:`ValueError` on a malformed path."""
    current: Optional[Variant] = root
    for kind, arg in parse(path):
        if current is None:
            return None
        if kind == "field":
            current = (current.get_field_by_key(arg)  # type: ignore[arg-type]
                       if current.get_type() == VariantType.OBJECT else None)
        else:  # "index"
            current = (current.get_element_at_index(arg)  # type: ignore[arg-type]
                       if current.get_type() == VariantType.ARRAY else None)
    return current


@lru_cache(maxsize=1000)
def parse(path: str) -> Tuple[Segment, ...]:
    """Parse ``path`` into segments. Cached, since rules usually pass a literal path that
    recurs per record. ``lru_cache`` does not cache exceptions, so a malformed path raises
    on every call (matching the Java LoadingCache behavior)."""
    if not path:
        raise ValueError("variant path must start with '$'")
    cur = _Cursor(path)
    if cur.peek() != "$":
        raise ValueError("variant path must start with '$', got: " + path)
    cur.next()
    out: List[Segment] = []
    while cur.has_more():
        ch = cur.peek()
        if ch == ".":
            cur.next()
            out.append(("field", _read_ident(cur, path)))
        elif ch == "[":
            cur.next()
            if not cur.has_more():
                raise ValueError("unexpected end of input after '[' in variant path: " + path)
            if cur.peek() in ("\"", "'"):
                out.append(("field", _read_quoted_key(cur, path)))
            else:
                out.append(("index", _read_index(cur, path)))
            if not cur.has_more() or cur.next() != "]":
                raise ValueError("expected ']' in variant path: " + path)
        else:
            raise ValueError("unexpected character '" + ch + "' in variant path: " + path)
    return tuple(out)


def _read_ident(cur: "_Cursor", path: str) -> str:
    if not cur.has_more() or not (cur.peek().isalpha() or cur.peek() == "_"):
        raise ValueError(
            "expected identifier (starting with a letter or '_') after '.' in variant path: "
            + path)
    start = cur.pos
    cur.next()
    while cur.has_more():
        ch = cur.peek()
        if ch.isalnum() or ch == "_":
            cur.next()
        else:
            break
    return cur.src[start:cur.pos]


def _read_quoted_key(cur: "_Cursor", path: str) -> str:
    quote = cur.next()
    out = []
    while cur.has_more():
        ch = cur.next()
        if ch == "\\":
            # Only two escapes are recognized: a doubled backslash for a literal backslash,
            # and backslash + the enclosing quote for a literal quote. Any other escape -
            # including a would-be Unicode escape like backslash-u00e9 - is a parse error
            # rather than being silently decoded to the wrong key. Literal characters
            # (including non-ASCII) need no escaping and pass through as-is.
            if not cur.has_more():
                raise ValueError(
                    "unterminated escape at end of quoted key in variant path: " + path)
            esc = cur.next()
            if esc == "\\" or esc == quote:
                out.append(esc)
            else:
                raise ValueError(
                    "unsupported escape '\\" + esc + "' in quoted key of variant path "
                    "(only '\\\\' and '\\" + quote + "' are allowed): " + path)
        elif ch == quote:
            return "".join(out)
        else:
            out.append(ch)
    raise ValueError("unterminated quoted key in variant path: " + path)


def _read_index(cur: "_Cursor", path: str) -> int:
    if cur.has_more() and cur.peek() == "-":
        raise ValueError("negative indices are not supported in variant path: " + path)
    start = cur.pos
    while cur.has_more() and cur.peek().isdigit():
        cur.next()
    if cur.pos == start:
        raise ValueError("expected integer index in variant path: " + path)
    return int(cur.src[start:cur.pos])


class _Cursor:
    __slots__ = ("src", "pos")

    def __init__(self, src: str):
        self.src = src
        self.pos = 0

    def has_more(self) -> bool:
        return self.pos < len(self.src)

    def peek(self) -> str:
        return self.src[self.pos]

    def next(self) -> str:
        ch = self.src[self.pos]
        self.pos += 1
        return ch
