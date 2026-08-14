import base64
import io
import sys
from collections import deque
from decimal import MAX_PREC, Context, Decimal
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

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
from google.protobuf import message_factory
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import DecodeError, EncodeError, Message
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
        # Driven by the runtime message's fields, each matched by name to the
        # schema-side descriptor, which is the one carrying the inline tags. The two
        # can differ under use.latest.version, and only the runtime field can be read
        # off the message. The schema field is resolved by number, not by name: protobuf
        # identifies a field by its number, and renaming a field at the same number is a
        # compatible change, so the registered schema's name for a field can differ.
        for fd in message.DESCRIPTOR.fields:
            schema_fd = descriptor.fields_by_number.get(fd.number)
            if schema_fd is None:
                # No schema field means no tags, so no transform applies to it.
                continue
            _transform_field(ctx, fd, schema_fd, descriptor, message, field_transform)
        return message
    field_ctx = ctx.current_field()
    if field_ctx is not None:
        rule_tags = ctx.rule.tags
        if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
            return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext,
    fd: FieldDescriptor,
    schema_fd: FieldDescriptor,
    desc: Descriptor,
    message: Message,
    field_transform: FieldTransform,
):
    try:
        # Names and tags come from the schema-side field descriptor - rules and metadata
        # tags are written against the registered schema; presence and the value itself
        # can only be read through the runtime one, which is carried along so that an
        # executor needing the field itself does not have to look the schema's name up on
        # the caller's message, where a compatible rename means it is not found.
        ctx.enter_field(
            message, schema_fd.full_name, schema_fd.name, get_type(fd), get_inline_tags(schema_fd), fd
        )
        # Skip-on-null, as in the validation walk: a field with explicit presence that is
        # unset has no value to transform, and writing one back would materialize it -
        # turning an absent message or unset optional scalar into a present one carrying a
        # transformed default. has_presence covers oneof members too.
        if fd.has_presence and not message.HasField(fd.name):
            return
        value = getattr(message, fd.name)
        if is_map_field(fd):
            value = {key: value[key] for key in value}
        elif _is_repeated(fd):
            value = [item for item in value]
        new_value = transform(ctx, _child_descriptor(schema_fd, desc), value, field_transform)
        if ctx.rule.kind == RuleKind.CONDITION:
            if new_value is False:
                raise RuleConditionError(ctx.rule)
        else:
            _set_field(fd, message, new_value)
    finally:
        ctx.exit_field()


def _child_descriptor(schema_fd: FieldDescriptor, desc: Descriptor) -> Descriptor:
    """
    The descriptor to walk a field's value with: the field's own message type for
    message-valued fields (a map's value type, since Python surfaces a map as a dict
    of values rather than a list of entries), and otherwise the containing descriptor,
    whose walk lands on the leaf branch and applies the transform.
    """
    if schema_fd.type != FieldDescriptor.TYPE_MESSAGE:
        return desc
    if is_map_field(schema_fd):
        value_fd = schema_fd.message_type.fields_by_name['value']
        return value_fd.message_type if value_fd.type == FieldDescriptor.TYPE_MESSAGE else desc
    return schema_fd.message_type


def _is_message_map(fd: FieldDescriptor) -> bool:
    return is_map_field(fd) and fd.message_type.fields_by_name['value'].type == FieldDescriptor.TYPE_MESSAGE


def _set_field(fd: FieldDescriptor, message: Message, value: Any):
    if isinstance(value, list):
        message.ClearField(fd.name)
        old_value = getattr(message, fd.name)
        old_value.extend(value)
    elif isinstance(value, dict):
        old_value = getattr(message, fd.name)
        if _is_message_map(fd):
            # A map of messages rejects update(); the walk transformed each entry in
            # place, so copying onto the live entry is a no-op unless the transform
            # handed back a different message.
            for key, item in value.items():
                old_value[key].CopyFrom(item)
        else:
            message.ClearField(fd.name)
            getattr(message, fd.name).update(value)
    elif isinstance(value, Message):
        # Message fields cannot be assigned; CopyFrom is a no-op when the walk
        # transformed the nested message in place and handed back the same object.
        getattr(message, fd.name).CopyFrom(value)
    else:
        setattr(message, fd.name, value)


# Keyed by (registered schema descriptor, runtime descriptor): whether a message with that
# runtime descriptor has to be re-read through the schema's before rules can bind `this` to
# it. The answer is no only for a class that describes the same fields as the registered
# schema; a class that has fallen behind it re-reads every record. See _needs_schema_view.
_SCHEMA_VIEW_NEEDED: Dict[Tuple[Descriptor, Descriptor], bool] = {}


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
    # The walk is driven by the caller's message throughout: it decides which fields exist,
    # which are absent, and what the values are. A rule that binds `this` to a message needs
    # one more thing - a view of that message in the schema's terms, since a rule's CEL
    # environment is built from the schema and `this.renamed` cannot read a field the
    # caller's class calls something else. Protobuf pairs fields by number on the wire, so
    # re-reading the message through the registered descriptor produces exactly that view.
    #
    # Whether that is needed is decided once per descriptor pair (see _needs_schema_view)
    # rather than per record. A generated class describing the same fields as the registered
    # schema skips it entirely, even though the two descriptors are distinct objects. A class
    # that has fallen behind the schema does not: under use.latest.version the schema may
    # declare a field the class has never heard of, and a rule that binds `this` can read the
    # schema's default for it, so those producers re-read every record. That cost is the price
    # of evaluating rules in the schema's terms, not an accident.
    schema_message = None
    if _needs_schema_view(descriptor, message.DESCRIPTOR):
        schema_message = message_factory.GetMessageClass(descriptor)()
        try:
            schema_message.ParseFromString(message.SerializeToString())
        except (DecodeError, EncodeError, UnicodeDecodeError) as e:
            # The bytes the producer is about to write cannot be read through the registered
            # schema, so a consumer reading with that schema could not read them either - a
            # bytes field carrying non-UTF-8 data against a schema that declares a string,
            # for instance, which is a compatible change. Fail in the channel the caller
            # already handles rather than leaking a protobuf DecodeError, and name the type
            # so it is searchable.
            raise SerializationError(
                f"Could not read message {descriptor.full_name} through the registered schema: {e}"
            ) from e
    _validate_message(executor, descriptor, message, "", fail_fast, violations, schema_message)
    return violations


def _needs_schema_view(descriptor: Descriptor, runtime_descriptor: Descriptor) -> bool:
    """
    Whether a message whose runtime descriptor is ``runtime_descriptor`` has to be re-read
    through ``descriptor`` before rules can bind ``this`` to it - true when the two disagree
    about any field a rule could observe: its name, its type, or whether it is repeated, at
    any depth.

    Presence deliberately does not count. Whether an unset field is absent is decided by the
    producer's field on the producer's message, which the walk reads directly, so a schema
    that only moved a field into or out of a oneof needs no re-read.

    A field the schema declares and the caller's class does not *does* count, which means a
    class running behind the registered schema - the use.latest.version case - re-reads every
    record. Only an exact match skips the re-read. Narrowing that to the rules that could
    actually observe the added field is possible but not simple: a rule binding ``this`` at
    any ancestor can traverse into the field, and a field-level rule on a message-valued field
    binds ``this`` to a type that need not declare rules of its own, so a per-descriptor test
    for message-level rules would be wrong in both directions.

    Memoized per descriptor pair: both are stable for the lifetime of a serializer, so this
    is one dict lookup per record rather than a tree comparison. The set of pairs a process
    sees is bounded by the message types it serializes.
    """
    if runtime_descriptor is descriptor:
        return False
    key = (descriptor, runtime_descriptor)
    needed = _SCHEMA_VIEW_NEEDED.get(key)
    if needed is None:
        needed = not _presents_same_values(descriptor, runtime_descriptor, set())
        _SCHEMA_VIEW_NEEDED[key] = needed
    return needed


def _presents_same_values(
    descriptor: Descriptor, runtime_descriptor: Descriptor, visited: Set[str]
) -> bool:
    """
    Whether the two descriptors present every field they share - paired by number, which is
    how protobuf identifies a field - under the same name, type and label, recursively
    through message-valued fields.

    A field the registered schema declares and the caller's does not counts as a difference:
    adding a field is a compatible change, and a message-level rule may reference the added
    field expecting the schema's default for it, which only a message read through the schema
    can supply. Fields only the caller declares are ignored - no rule can name them, and the
    walk skips them.

    ``visited`` holds the descriptor pairs already compared, so a self-referential message
    type terminates.
    """
    pair = f"{descriptor.full_name} {runtime_descriptor.full_name}"
    if pair in visited:
        # Already compared on another path, or cycling back to it. Either way this pair
        # contributes no new disagreement.
        return True
    visited.add(pair)
    for schema_fd in descriptor.fields:
        if runtime_descriptor.fields_by_number.get(schema_fd.number) is None:
            return False
    for runtime_fd in runtime_descriptor.fields:
        schema_fd = descriptor.fields_by_number.get(runtime_fd.number)
        if schema_fd is None:
            continue
        if (
            schema_fd.name != runtime_fd.name
            or schema_fd.type != runtime_fd.type
            or _is_repeated(schema_fd) != _is_repeated(runtime_fd)
        ):
            return False
        if runtime_fd.type == FieldDescriptor.TYPE_MESSAGE and not _presents_same_values(
            schema_fd.message_type, runtime_fd.message_type, visited
        ):
            return False
    return True


def _validate_message(
    executor: ValidationRuleExecutor,
    descriptor: Descriptor,
    message: Any,
    path: str,
    fail_fast: bool,
    out: List[ValidationRuleError],
    schema_message: Optional[Any] = None,
):
    """
    Mirrors :func:`transform`'s dispatch shape, walking the message's fields and
    descending into message-valued fields, map values and repeated elements.

    The walk is driven by the caller's ``message``: it decides which fields exist, which are
    absent, and what the values are. Each field is paired to ``descriptor`` by number, which
    is how protobuf identifies a field, and the schema's field supplies the rules and the name
    used in the reported path. Fields the schema does not declare are skipped, so the walk
    visits the intersection - the same fields the transform walk visits.

    ``schema_message`` is the same message read through ``descriptor``, or ``None`` when the
    two descriptors present it identically. It is used only where a rule binds ``this`` to a
    message, which is the one place the schema's field names matter.
    """
    if descriptor is None or message is None or not isinstance(message, Message):
        return
    # Message-level rules: this = the message, read as the schema names it.
    for rule in _read_message_validation_rules(descriptor):
        evaluate_validation_rule(
            executor,
            rule,
            descriptor,
            message if schema_message is None else schema_message,
            path,
            out,
        )
        if fail_fast and out:
            return
    for fd in message.DESCRIPTOR.fields:
        schema_fd = descriptor.fields_by_number.get(fd.number)
        if schema_fd is None:
            # The registered schema does not declare this field, so it carries no rules and
            # nothing below it can either.
            continue
        # Skip-on-null: a field with explicit presence that is unset does not invoke
        # the executor. Repeated/map fields have no presence and are never None.
        #
        # Both halves are read from the caller's message: whether an unset field counts as
        # absent is decided by the class that wrote it, not by the registered schema, and the
        # two can disagree - moving a field into or out of a oneof is a compatible change.
        if fd.has_presence and not message.HasField(fd.name):
            continue
        value = getattr(message, fd.name)
        # The path names the field as the registered schema does, which is what a rule refers
        # to; the value is still read through the caller's field.
        child_path = schema_fd.name if not path else f"{path}.{schema_fd.name}"
        # Where a schema view exists, every value comes from it, not just message-valued
        # ones: the two descriptors can disagree about representation as well as naming.
        # bytes and string are interchangeable at the same number - a compatible change -
        # and a rule authored as `this == 'hello'` cannot match a bytes value. Reading the
        # field is cheap next to the re-read that already happened.
        schema_value = None if schema_message is None else getattr(schema_message, schema_fd.name)
        for rule in _read_field_validation_rules(schema_fd):
            evaluate_validation_rule(
                executor,
                rule,
                schema_fd,
                value if schema_value is None else schema_value,
                child_path,
                out,
            )
            if fail_fast and out:
                return
        if fd.type != FieldDescriptor.TYPE_MESSAGE or schema_fd.type != FieldDescriptor.TYPE_MESSAGE:
            continue
        if is_map_field(fd):
            value_fd = fd.message_type.fields_by_name['value']
            schema_value_fd = schema_fd.message_type.fields_by_name['value']
            if (
                value_fd.type == FieldDescriptor.TYPE_MESSAGE
                and schema_value_fd.type == FieldDescriptor.TYPE_MESSAGE
            ):
                for key, item in value.items():
                    # Map values pair by key rather than position.
                    schema_item = (
                        schema_value[key]
                        if schema_value is not None and key in schema_value
                        else None
                    )
                    _validate_message(
                        executor,
                        schema_value_fd.message_type,
                        item,
                        f'{child_path}["{key}"]',
                        fail_fast,
                        out,
                        schema_item,
                    )
                    if fail_fast and out:
                        return
        elif _is_repeated(fd):
            for i, item in enumerate(value):
                # Both lists came from the same bytes, so they line up; the guard is for
                # safety.
                schema_item = (
                    schema_value[i]
                    if schema_value is not None and i < len(schema_value)
                    else None
                )
                _validate_message(
                    executor,
                    schema_fd.message_type,
                    item,
                    f"{child_path}[{i}]",
                    fail_fast,
                    out,
                    schema_item,
                )
                if fail_fast and out:
                    return
        else:
            _validate_message(
                executor, schema_fd.message_type, value, child_path, fail_fast, out, schema_value
            )
            if fail_fast and out:
                return


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
    # Read the options via GetOptions() rather than the deprecated `options` attribute,
    # which is absent from the upb descriptors used by protobuf >= 7 — where reading it
    # made this return False for every map field.
    return fd.type == FieldDescriptor.TYPE_MESSAGE and fd.message_type.GetOptions().map_entry


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
