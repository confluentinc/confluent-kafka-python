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

"""Shared utility for parsing ``key=value`` strings."""

import re
from typing import Iterable, Iterator, Optional, Tuple

__all__ = ["parse_kv"]


def parse_kv(
    raw: str,
    separators: Iterable[str],
    context_label: Optional[str] = None,
    trim_tokens: bool = True,
) -> Iterator[Tuple[str, str]]:
    """Tokenize ``raw`` and yield each non-empty token as a ``(key, value)`` pair.

    Tokens are split on any character in ``separators`` (e.g. ``[',']`` for
    comma-separated values or ``[' ', '\\t', '\\r', '\\n']`` for
    whitespace-separated). Within each token the split is on the first ``=``
    only — values may legitimately contain ``=`` (e.g. URL query strings).

    Empty tokens (e.g. consecutive separators, or whitespace-only tokens when
    ``trim_tokens`` is true) are skipped. Tokens with no ``=`` or with ``=``
    at position 0 (empty key) raise :class:`ValueError` with
    ``context_label`` woven into the message when supplied.

    The default trimming behaviour mirrors librdkafka's
    ``rd_string_split`` (``rdstring.c``).

    :param raw: Input string to tokenize.
    :param separators: Iterable of single-character separators. Each element
        may be a string of any length, but only its first character is used.
    :param context_label: Optional label woven into error messages to
        identify which config the malformed token came from. When ``None``,
        error messages fall back to a generic ``"key=value"`` phrasing.
    :param trim_tokens: When true (default), each token is stripped of
        leading and trailing whitespace before being split on ``=``. Set to
        false to preserve whitespace inside tokens.
    :raises TypeError: ``raw`` or ``separators`` is ``None``.
    :raises ValueError: A token is malformed (no ``=`` or empty key).
    """
    if raw is None:
        raise TypeError("raw must not be None")
    if separators is None:
        raise TypeError("separators must not be None")

    # Build a single-character split pattern from the supplied separators.
    chars = "".join(str(s)[0] for s in separators if s)
    if not chars:
        # Degenerate "no separators" → treat raw as a single token.
        raw_tokens = [raw]
    else:
        raw_tokens = re.split("[" + re.escape(chars) + "]", raw)

    for raw_token in raw_tokens:
        token = raw_token.strip() if trim_tokens else raw_token
        if len(token) == 0:
            continue

        idx = token.find("=")
        if idx <= 0:
            what = f"'{context_label}'" if context_label else "key=value"
            raise ValueError(f"Malformed {what} entry '{token}' (expected key=value).")

        yield token[:idx], token[idx + 1 :]
