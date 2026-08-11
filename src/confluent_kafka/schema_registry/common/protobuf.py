import base64
import io
import sys
from collections import deque
from decimal import MAX_PREC, Context, Decimal
from typing import Any, Deque, List, Optional, Set

from google.protobuf import __version__ as _protobuf_version
from google.protobuf import (
    any_pb2,
    api_pb2,
    descriptor_pb2,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    source_context_pb2,
    struct_pb2,
    timestamp_pb2,
    type_pb2,
    wrappers_pb2,
)
from google.protobuf.descriptor import Descriptor, FieldDescriptor, FileDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import DecodeError, Message
from google.type import (
    calendar_period_pb2,
    color_pb2,
    date_pb2,
    datetime_pb2,
    dayofweek_pb2,
    expr_pb2,
    fraction_pb2,
    latlng_pb2,
    money_pb2,
    month_pb2,
    postal_address_pb2,
    quaternion_pb2,
    timeofday_pb2,
)

import confluent_kafka.schema_registry.confluent.meta_pb2 as meta_pb2
from confluent_kafka.schema_registry import RuleKind
from confluent_kafka.schema_registry.confluent.types import decimal_pb2
from confluent_kafka.schema_registry.serde import (
    FieldTransform,
    FieldType,
    RuleConditionError,
    RuleContext,
    ValidationRule,
    ValidationRuleError,
    ValidationRuleExecutor,
    evaluate_validation_rule,
)
from confluent_kafka.serialization import SerializationError

__all__ = [
    '_bytes',
    '_create_index_array',
    '_schema_to_str',
    '_proto_to_str',
    '_str_to_proto',
    '_init_pool',
    'transform',
    '_transform_field',
    '_set_field',
    'validate_message',
    'get_type',
    'is_map_field',
    '_is_repeated',
    'get_inline_tags',
    '_disjoint',
    '_is_builtin',
    'decimal_to_protobuf',
    'protobuf_to_decimal',
]

# Convert an int to bytes (inverse of ord())
# Python3.chr() -> Unicode
# Python2.chr() -> str(alias for bytes)
if sys.version > '3':

    def _bytes(v: int) -> bytes:
        """
        Convert int to bytes

        Args:
            v (int): The int to convert to bytes.
        """
        return bytes((v,))

else:

    def _bytes(v: int) -> str:  # type: ignore[misc]
        """
        Convert int to bytes

        Args:
            v (int): The int to convert to bytes.
        """
        return chr(v)


PROTOBUF_TYPE = "PROTOBUF"

# protobuf 7 removed the deprecated FieldDescriptor.label property in favor of the
# is_repeated/is_required boolean properties. Track the major version so we keep
# working on both old (<7, has .label) and new (>=7, only .is_repeated) runtimes.
PROTOBUF_MAJOR_VERSION = int(_protobuf_version.split('.')[0])


def _is_repeated(fd: FieldDescriptor) -> bool:
    if PROTOBUF_MAJOR_VERSION >= 7:
        return fd.is_repeated
    return fd.label == FieldDescriptor.LABEL_REPEATED


class _ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _create_index_array(msg_desc: Descriptor) -> List[int]:
    """
    Creates an index array specifying the location of msg_desc in
    the referenced FileDescriptor.

    Args:
        msg_desc (MessageDescriptor): Protobuf MessageDescriptor

    Returns:
        list of int: Protobuf MessageDescriptor index array.

    Raises:
        ValueError: If the message descriptor is malformed.
    """

    msg_idx: Deque[int] = deque()

    # Walk the nested MessageDescriptor tree up to the root.
    current = msg_desc
    found = False
    while current.containing_type is not None:
        previous = current
        current = previous.containing_type
        # find child's position
        for idx, node in enumerate(current.nested_types):
            if node == previous:
                msg_idx.appendleft(idx)
                found = True
                break
        if not found:
            raise ValueError("Nested MessageDescriptor not found")

    # Add the index of the root MessageDescriptor in the FileDescriptor.
    found = False
    for idx, msg_type_name in enumerate(msg_desc.file.message_types_by_name):
        if msg_type_name == current.name:
            msg_idx.appendleft(idx)
            found = True
            break
    if not found:
        raise ValueError("MessageDescriptor not found in file")

    return list(msg_idx)


def _schema_to_str(file_descriptor: FileDescriptor) -> str:
    """
    Base64 encode a FileDescriptor

    Args:
        file_descriptor (FileDescriptor): FileDescriptor to encode.

    Returns:
        str: Base64 encoded FileDescriptor
    """

    return base64.standard_b64encode(file_descriptor.serialized_pb).decode('ascii')


def _proto_to_str(file_descriptor_proto: descriptor_pb2.FileDescriptorProto) -> str:
    """
    Base64 encode a FileDescriptorProto

    Args:
        file_descriptor_proto (FileDescriptorProto): FileDescriptorProto to encode.

    Returns:
        str: Base64 encoded FileDescriptorProto
    """

    return base64.standard_b64encode(file_descriptor_proto.SerializeToString()).decode('ascii')


def _str_to_proto(name: str, schema_str: str) -> descriptor_pb2.FileDescriptorProto:
    """
    Base64 decode a FileDescriptor

    Args:
        schema_str (str): Base64 encoded FileDescriptorProto

    Returns:
        FileDescriptorProto: schema.
    """

    serialized_pb = base64.standard_b64decode(schema_str.encode('ascii'))
    file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
    try:
        file_descriptor_proto.ParseFromString(serialized_pb)
        file_descriptor_proto.name = name
    except DecodeError as e:
        raise SerializationError(str(e))
    return file_descriptor_proto


def _init_pool(pool: DescriptorPool):
    pool.AddSerializedFile(any_pb2.DESCRIPTOR.serialized_pb)
    # source_context needed by api
    pool.AddSerializedFile(source_context_pb2.DESCRIPTOR.serialized_pb)
    # type needed by api
    pool.AddSerializedFile(type_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(api_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(descriptor_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(duration_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(empty_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(field_mask_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(struct_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(timestamp_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(wrappers_pb2.DESCRIPTOR.serialized_pb)

    pool.AddSerializedFile(calendar_period_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(color_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(date_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(datetime_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(dayofweek_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(expr_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(fraction_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(latlng_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(money_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(month_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(postal_address_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(quaternion_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(timeofday_pb2.DESCRIPTOR.serialized_pb)

    pool.AddSerializedFile(meta_pb2.DESCRIPTOR.serialized_pb)
    pool.AddSerializedFile(decimal_pb2.DESCRIPTOR.serialized_pb)


def transform(ctx: RuleContext, descriptor: Descriptor, message: Any, field_transform: FieldTransform) -> Any:
    if message is None or descriptor is None:
        return message
    if isinstance(message, list):
        return [transform(ctx, descriptor, item, field_transform) for item in message]
    if isinstance(message, dict):
        return {key: transform(ctx, descriptor, value, field_transform) for key, value in message.items()}
    if isinstance(message, Message):
        for fd in descriptor.fields:
            _transform_field(ctx, fd, descriptor, message, field_transform)
        return message
    field_ctx = ctx.current_field()
    if field_ctx is not None:
        rule_tags = ctx.rule.tags
        if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
            return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext, fd: FieldDescriptor, desc: Descriptor, message: Message, field_transform: FieldTransform
):
    try:
        ctx.enter_field(message, fd.full_name, fd.name, get_type(fd), get_inline_tags(fd))
        if fd.containing_oneof is not None and not message.HasField(fd.name):
            return
        value = getattr(message, fd.name)
        if is_map_field(fd):
            value = {key: value[key] for key in value}
        elif _is_repeated(fd):
            value = [item for item in value]
        new_value = transform(ctx, desc, value, field_transform)
        if ctx.rule.kind == RuleKind.CONDITION:
            if new_value is False:
                raise RuleConditionError(ctx.rule)
        else:
            _set_field(fd, message, new_value)
    finally:
        ctx.exit_field()


def _set_field(fd: FieldDescriptor, message: Message, value: Any):
    if isinstance(value, list):
        message.ClearField(fd.name)
        old_value = getattr(message, fd.name)
        old_value.extend(value)
    elif isinstance(value, dict):
        message.ClearField(fd.name)
        old_value = getattr(message, fd.name)
        old_value.update(value)
    else:
        setattr(message, fd.name, value)


def validate_message(
    executor: Optional[ValidationRuleExecutor],
    descriptor: Optional[Descriptor],
    message: Any,
    fail_fast: bool = False,
) -> List[ValidationRuleError]:
    """
    Walk ``message`` against ``descriptor``, evaluating every inline validation rule
    declared in the ``confluent.Meta`` extension and collecting all failures.
    Read-only — the message is not modified.

    Two kinds of rules are evaluated:

    - Message-level (``confluent.message_meta`` rules) — ``this`` is the message.
    - Field-level (``confluent.field_meta`` rules) — ``this`` is the field value; for
      repeated and map fields that is the whole collection. Honors the skip-on-null
      contract: a field with explicit presence that is unset (proto3 ``optional``,
      singular message fields, oneof members) does not have its rules invoked.

    Failures are appended with their dotted-path location (e.g. ``addr.zip``,
    ``items[3]``, ``labels["k"]``). The walk continues after each failure unless
    ``fail_fast`` is set.

    Only ``message_meta`` and ``field_meta`` rules are evaluated; rules on files,
    enums and enum values are ignored, matching the JVM client.
    """
    violations: List[ValidationRuleError] = []
    if executor is None or descriptor is None or message is None:
        return violations
    _validate_message(executor, descriptor, message, "", fail_fast, violations)
    return violations


def _validate_message(
    executor: ValidationRuleExecutor,
    descriptor: Descriptor,
    message: Any,
    path: str,
    fail_fast: bool,
    out: List[ValidationRuleError],
):
    """
    Mirrors :func:`transform`'s dispatch shape, walking the descriptor's fields and
    descending into message-valued fields, map values and repeated elements.
    """
    if descriptor is None or message is None or not isinstance(message, Message):
        return
    # Message-level rules: this = the message.
    for rule in _read_message_validation_rules(descriptor):
        evaluate_validation_rule(executor, rule, descriptor, message, path, out)
        if fail_fast and out:
            return
    for fd in descriptor.fields:
        # Skip-on-null: a field with explicit presence that is unset does not invoke
        # the executor. Repeated/map fields have no presence and are never None.
        if fd.has_presence and not message.HasField(fd.name):
            continue
        value = getattr(message, fd.name)
        child_path = fd.name if not path else f"{path}.{fd.name}"
        for rule in _read_field_validation_rules(fd):
            evaluate_validation_rule(executor, rule, fd, value, child_path, out)
            if fail_fast and out:
                return
        if fd.type != FieldDescriptor.TYPE_MESSAGE:
            continue
        if _is_map_field(fd):
            value_fd = fd.message_type.fields_by_name['value']
            if value_fd.type == FieldDescriptor.TYPE_MESSAGE:
                for key, item in value.items():
                    _validate_message(
                        executor, value_fd.message_type, item, f'{child_path}["{key}"]', fail_fast, out
                    )
                    if fail_fast and out:
                        return
        elif _is_repeated(fd):
            for i, item in enumerate(value):
                _validate_message(executor, fd.message_type, item, f"{child_path}[{i}]", fail_fast, out)
                if fail_fast and out:
                    return
        else:
            _validate_message(executor, fd.message_type, value, child_path, fail_fast, out)
            if fail_fast and out:
                return


def _is_map_field(fd: FieldDescriptor) -> bool:
    """
    True if ``fd`` is a map field.

    Deliberately does not reuse :func:`is_map_field`, which reads the deprecated
    ``Descriptor.options`` attribute — absent from the upb descriptors used by
    protobuf >= 7, where it therefore reports False for every map field.
    """
    return fd.type == FieldDescriptor.TYPE_MESSAGE and fd.message_type.GetOptions().map_entry


def _read_message_validation_rules(descriptor: Descriptor) -> List[ValidationRule]:
    options = descriptor.GetOptions()
    if not options.HasExtension(meta_pb2.message_meta):  # type: ignore[attr-defined]
        return []
    return _to_validation_rules(options.Extensions[meta_pb2.message_meta].rules)  # type: ignore[attr-defined]


def _read_field_validation_rules(fd: FieldDescriptor) -> List[ValidationRule]:
    options = fd.GetOptions()
    if not options.HasExtension(meta_pb2.field_meta):  # type: ignore[attr-defined]
        return []
    return _to_validation_rules(options.Extensions[meta_pb2.field_meta].rules)  # type: ignore[attr-defined]


def _to_validation_rules(rules: Any) -> List[ValidationRule]:
    return [ValidationRule(r.name, r.doc, r.expr, r.sql) for r in rules]


def get_type(fd: FieldDescriptor) -> FieldType:
    if is_map_field(fd):
        return FieldType.MAP
    if fd.type == FieldDescriptor.TYPE_MESSAGE:
        return FieldType.RECORD
    if fd.type == FieldDescriptor.TYPE_ENUM:
        return FieldType.ENUM
    if fd.type == FieldDescriptor.TYPE_STRING:
        return FieldType.STRING
    if fd.type == FieldDescriptor.TYPE_BYTES:
        return FieldType.BYTES
    if fd.type in (
        FieldDescriptor.TYPE_INT32,
        FieldDescriptor.TYPE_SINT32,
        FieldDescriptor.TYPE_UINT32,
        FieldDescriptor.TYPE_FIXED32,
        FieldDescriptor.TYPE_SFIXED32,
    ):
        return FieldType.INT
    if fd.type in (
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_SINT64,
        FieldDescriptor.TYPE_UINT64,
        FieldDescriptor.TYPE_FIXED64,
        FieldDescriptor.TYPE_SFIXED64,
    ):
        return FieldType.LONG
    if fd.type == FieldDescriptor.TYPE_FLOAT:
        return FieldType.FLOAT
    if fd.type == FieldDescriptor.TYPE_DOUBLE:
        return FieldType.DOUBLE
    if fd.type == FieldDescriptor.TYPE_BOOL:
        return FieldType.BOOLEAN
    return FieldType.NULL


def is_map_field(fd: FieldDescriptor):
    return (
        fd.type == FieldDescriptor.TYPE_MESSAGE
        and hasattr(fd.message_type, 'options')
        and fd.message_type.options.map_entry
    )


def get_inline_tags(fd: FieldDescriptor) -> Set[str]:
    meta = fd.GetOptions().Extensions[meta_pb2.field_meta]  # type: ignore[attr-defined]
    if meta is None:
        return set()
    else:
        return set(meta.tags)


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def _is_builtin(name: str) -> bool:
    return name.startswith('confluent/') or name.startswith('google/protobuf/') or name.startswith('google/type/')


def decimal_to_protobuf(value: Decimal, scale: int) -> decimal_pb2.Decimal:  # type: ignore[name-defined]
    """
    Converts a Decimal to a Protobuf value.

    Args:
        value (Decimal): The Decimal value to convert.
        scale (int): The number of decimal points to convert.

    Returns:
        The Protobuf value.
    """
    sign, digits, exp = value.as_tuple()

    delta = exp + scale  # type: ignore[operator]

    if delta < 0:
        raise ValueError("Scale provided does not match the decimal")

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    unscaled_datum = 10**delta * unscaled_datum

    bytes_req = (unscaled_datum.bit_length() + 8) // 8

    if sign:
        unscaled_datum = -unscaled_datum

    bytes = unscaled_datum.to_bytes(bytes_req, byteorder="big", signed=True)

    result = decimal_pb2.Decimal()  # type: ignore[attr-defined]
    result.value = bytes
    result.precision = 0
    result.scale = scale
    return result


decimal_context = Context()


def protobuf_to_decimal(value: decimal_pb2.Decimal) -> Decimal:  # type: ignore[name-defined]
    """
    Converts a Protobuf value to Decimal.

    Args:
        value (decimal_pb2.Decimal): The Protobuf value to convert.

    Returns:
        The Decimal value.
    """
    unscaled_datum = int.from_bytes(value.value, byteorder="big", signed=True)

    if value.precision > 0:
        decimal_context.prec = value.precision
    else:
        decimal_context.prec = MAX_PREC
    return decimal_context.create_decimal(unscaled_datum).scaleb(-value.scale, decimal_context)
