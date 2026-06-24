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

"""Frozen cross-module contract guard for the autowire entry-point.

These tests guard against accidental drift: parameter names, type
annotations, return annotation, arity, and absence of defaults.
"""

import inspect
from typing import Callable, Dict, Optional, Tuple

import pytest

pytest.importorskip("boto3")

from confluent_kafka._oauthbearer.aws import aws_autowire  # noqa: E402
from confluent_kafka._oauthbearer.aws.aws_autowire import OAuthBearerCallback, create_handler  # noqa: E402

# ---- Module surface ----


def test_module_importable_at_canonical_path():
    """The C dispatcher does PyImport_ImportModule(...) with this exact path."""
    import importlib

    mod = importlib.import_module("confluent_kafka._oauthbearer.aws.aws_autowire")
    assert mod is aws_autowire


def test_create_handler_is_module_level_callable():
    assert callable(aws_autowire.create_handler)
    assert aws_autowire.create_handler is create_handler


def test_oauthbearer_callback_type_alias_exported():
    assert hasattr(aws_autowire, "OAuthBearerCallback")
    assert aws_autowire.OAuthBearerCallback is OAuthBearerCallback


def test_module_all_lists_public_surface():
    """__all__ should advertise only the two public names."""
    assert set(aws_autowire.__all__) == {"create_handler", "OAuthBearerCallback"}


# ---- create_handler frozen signature ----


def test_create_handler_arity_is_two():
    sig = inspect.signature(create_handler)
    assert len(sig.parameters) == 2


def test_create_handler_parameter_names_are_frozen():
    sig = inspect.signature(create_handler)
    names = list(sig.parameters.keys())
    assert names == ["sasl_oauthbearer_config", "sasl_oauthbearer_extensions"]


def test_create_handler_parameter_annotations_are_frozen():
    sig = inspect.signature(create_handler)
    assert sig.parameters["sasl_oauthbearer_config"].annotation is str
    assert sig.parameters["sasl_oauthbearer_extensions"].annotation == Optional[str]


def test_create_handler_no_default_values():
    """Both parameters must be required positional — the C dispatcher always
    passes two arguments (extensions may be the empty string or None, but
    is always supplied)."""
    sig = inspect.signature(create_handler)
    assert sig.parameters["sasl_oauthbearer_config"].default is inspect.Parameter.empty
    assert sig.parameters["sasl_oauthbearer_extensions"].default is inspect.Parameter.empty


def test_create_handler_return_annotation_is_oauthbearer_callback():
    sig = inspect.signature(create_handler)
    assert sig.return_annotation is OAuthBearerCallback


def test_create_handler_parameters_accept_positional_arguments():
    """The C dispatcher calls via PyObject_CallFunction with positional args."""
    sig = inspect.signature(create_handler)
    for name in ["sasl_oauthbearer_config", "sasl_oauthbearer_extensions"]:
        kind = sig.parameters[name].kind
        # POSITIONAL_OR_KEYWORD covers what PyObject_CallFunction provides.
        assert kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )


# ---- OAuthBearerCallback shape (the type returned by create_handler) ----


def test_oauthbearer_callback_matches_c_oauth_cb_contract():
    """The C oauth_cb wrapper at confluent_kafka.c:2291 does:

        PyArg_ParseTuple(result, "sd|sO!", &token, &expiry, &principal, &PyDict_Type, &extensions)

    So the callable returned by create_handler must accept one string
    argument (the sasl.oauthbearer.config pass-through) and return a tuple
    of (str, float, str, Dict[str, str]).
    """
    expected = Callable[[str], Tuple[str, float, str, Dict[str, str]]]
    assert OAuthBearerCallback == expected


# ---- create_handler docstring sanity ----


def test_create_handler_docstring_present():
    """The frozen cross-module contract is documented in the function's
    docstring so it's visible to anyone reading the source. Block a future
    contributor from accidentally stripping the docstring."""
    assert create_handler.__doc__ is not None
    assert "frozen" in aws_autowire.__doc__.lower()


def test_create_handler_docstring_references_key_contract_terms():
    doc = create_handler.__doc__ or ""
    # Names of the two arguments must appear so users grepping for them
    # find the documentation.
    assert "sasl.oauthbearer.config" in doc
    assert "sasl.oauthbearer.extensions" in doc
