#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
#
import pytest
from confluent_kafka import TopicPartition

from confluent_kafka.error import ConsumeError, ValueSerializationError
from confluent_kafka.schema_registry import SchemaReference, Schema
from confluent_kafka.schema_registry.json_schema import (JSONSerializer,
                                                         JSONDeserializer)


class _TestProduct(object):
    def __init__(self, product_id, name, price, tags, dimensions, location):
        self.product_id = product_id
        self.name = name
        self.price = price
        self.tags = tags
        self.dimensions = dimensions
        self.location = location

    def __eq__(self, other):
        return all([
            self.product_id == other.product_id,
            self.name == other.name,
            self.price == other.price,
            self.tags == other.tags,
            self.dimensions == other.dimensions,
            self.location == other.location
        ])


class _TestCustomer(object):
    def __init__(self, name, id):
        self.name = name
        self.id = id

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.id == other.id
        ])


class _TestOrderDetails(object):
    def __init__(self, id, customer):
        self.id = id
        self.customer = customer

    def __eq__(self, other):
        return all([
            self.id == other.id,
            self.customer == other.customer
        ])


class _TestOrder(object):
    def __init__(self, order_details, product):
        self.order_details = order_details
        self.product = product

    def __eq__(self, other):
        return all([
            self.order_details == other.order_details,
            self.product == other.product
        ])


class _TestReferencedProduct(object):
    def __init__(self, name, product):
        self.name = name
        self.product = product

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.product == other.product
        ])


def _testProduct_to_dict(product_obj, ctx):
    """
    Returns testProduct instance in dict format.

    Args:
        product_obj (_TestProduct): testProduct instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        dict: product_obj as a dictionary.

    """
    return {"productId": product_obj.product_id,
            "productName": product_obj.name,
            "price": product_obj.price,
            "tags": product_obj.tags,
            "dimensions": product_obj.dimensions,
            "warehouseLocation": product_obj.location}


def _testCustomer_to_dict(customer_obj, ctx):
    """
    Returns testCustomer instance in dict format.

    Args:
        customer_obj (_TestCustomer): testCustomer instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        dict: customer_obj as a dictionary.

    """
    return {"name": customer_obj.name,
            "id": customer_obj.id}


def _testOrderDetails_to_dict(orderdetails_obj, ctx):
    """
    Returns testOrderDetails instance in dict format.

    Args:
        orderdetails_obj (_TestOrderDetails): testOrderDetails instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        dict: orderdetails_obj as a dictionary.

    """
    return {"id": orderdetails_obj.id,
            "customer": _testCustomer_to_dict(orderdetails_obj.customer, ctx)}


def _testOrder_to_dict(order_obj, ctx):
    """
    Returns testOrder instance in dict format.

    Args:
        order_obj (_TestOrder): testOrder instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        dict: order_obj as a dictionary.

    """
    return {"order_details": _testOrderDetails_to_dict(order_obj.order_details, ctx),
            "product": _testProduct_to_dict(order_obj.product, ctx)}


def _testProduct_from_dict(product_dict, ctx):
    """
    Returns testProduct instance from its dict format.

    Args:
        product_dict (dict): testProduct in dict format.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        _TestProduct: product_obj instance.

    """
    return _TestProduct(product_dict['productId'],
                        product_dict['productName'],
                        product_dict['price'],
                        product_dict['tags'],
                        product_dict['dimensions'],
                        product_dict['warehouseLocation'])


def _testCustomer_from_dict(customer_dict, ctx):
    """
    Returns testCustomer instance from its dict format.

    Args:
        customer_dict (dict): testCustomer in dict format.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        _TestCustomer: customer_obj instance.

    """
    return _TestCustomer(customer_dict['name'],
                         customer_dict['id'])


def _testOrderDetails_from_dict(orderdetails_dict, ctx):
    """
    Returns testOrderDetails instance from its dict format.

    Args:
        orderdetails_dict (dict): testOrderDetails in dict format.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        _TestOrderDetails: orderdetails_obj instance.

    """
    return _TestOrderDetails(orderdetails_dict['id'],
                             _testCustomer_from_dict(orderdetails_dict['customer'], ctx))


def _testOrder_from_dict(order_dict, ctx):
    """
    Returns testOrder instance from its dict format.

    Args:
        order_dict (dict): testOrder in dict format.

        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

    Returns:
        _TestOrder: order_obj instance.

    """
    return _TestOrder(_testOrderDetails_from_dict(order_dict['order_details'], ctx),
                      _testProduct_from_dict(order_dict['product'], ctx))


def test_json_record_serialization(kafka_cluster, load_file):
    """
    Tests basic JsonSerializer and JsonDeserializer basic functionality.

    product.json from:
        https://json-schema.org/learn/getting-started-step-by-step.html

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    schema_str = load_file("product.json")
    value_serializer = JSONSerializer(schema_str, sr)
    value_deserializer = JSONDeserializer(schema_str)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = {"productId": 1,
              "productName": "An ice sculpture",
              "price": 12.50,
              "tags": ["cold", "ice"],
              "dimensions": {
                  "length": 7.0,
                  "width": 12.0,
                  "height": 9.5
              },
              "warehouseLocation": {
                  "latitude": -78.75,
                  "longitude": 20.4
              }}

    producer.produce(topic, value=record, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert all([actual[k] == v for k, v in record.items()])


def test_json_record_serialization_incompatible(kafka_cluster, load_file):
    """
    Tests Serializer validation functionality.

    product.json from:
        https://json-schema.org/learn/getting-started-step-by-step.html

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    schema_str = load_file("product.json")
    value_serializer = JSONSerializer(schema_str, sr)
    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = {"contractorId": 1,
              "contractorName": "David Davidson",
              "contractRate": 1250,
              "trades": ["mason"]}

    with pytest.raises(ValueSerializationError,
                       match=r"(.*) is a required property"):
        producer.produce(topic, value=record, partition=0)


def test_json_record_serialization_no_title(kafka_cluster, load_file):
    """
    Ensures ValueError raise if JSON Schema definition lacks Title annotation.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    sr = kafka_cluster.schema_registry()
    schema_str = load_file('not_title.json')

    with pytest.raises(ValueError,
                       match="Missing required JSON schema annotation title"):
        JSONSerializer(schema_str, sr)


def test_json_record_serialization_custom(kafka_cluster, load_file):
    """
    Ensures to_dict and from_dict hooks are properly applied by the serializer.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    schema_str = load_file("product.json")
    value_serializer = JSONSerializer(schema_str, sr,
                                      to_dict=_testProduct_to_dict)
    value_deserializer = JSONDeserializer(schema_str,
                                          from_dict=_testProduct_from_dict)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = _TestProduct(product_id=1,
                          name="The ice sculpture",
                          price=12.50,
                          tags=["cold", "ice"],
                          dimensions={"length": 7.0,
                                      "width": 12.0,
                                      "height": 9.5},
                          location={"latitude": -78.75,
                                    "longitude": 20.4})

    producer.produce(topic, value=record, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert all([getattr(actual, attribute) == getattr(record, attribute)
                for attribute in vars(record)])


def test_json_record_deserialization_mismatch(kafka_cluster, load_file):
    """
    Ensures to_dict and from_dict hooks are properly applied by the serializer.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    schema_str = load_file("contractor.json")
    schema_str2 = load_file("product.json")

    value_serializer = JSONSerializer(schema_str, sr)
    value_deserializer = JSONDeserializer(schema_str2)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = {"contractorId": 2,
              "contractorName": "Magnus Edenhill",
              "contractRate": 30,
              "trades": ["pickling"]}

    producer.produce(topic, value=record, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    with pytest.raises(
            ConsumeError,
            match="'productId' is a required property"):
        consumer.poll()


def _register_referenced_schemas(sr, load_file):
    sr.register_schema("product", Schema(load_file("product.json"), 'JSON'))
    sr.register_schema("customer", Schema(load_file("customer.json"), 'JSON'))
    sr.register_schema("order_details", Schema(load_file("order_details.json"), 'JSON', [
        SchemaReference("http://example.com/customer.schema.json", "customer", 1)]))

    order_schema = Schema(load_file("order.json"), 'JSON',
                          [SchemaReference("http://example.com/order_details.schema.json", "order_details", 1),
                           SchemaReference("http://example.com/product.schema.json", "product", 1)])
    return order_schema


def test_json_reference(kafka_cluster, load_file):
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    product = {"productId": 1,
               "productName": "An ice sculpture",
               "price": 12.50,
               "tags": ["cold", "ice"],
               "dimensions": {
                   "length": 7.0,
                   "width": 12.0,
                   "height": 9.5
               },
               "warehouseLocation": {
                   "latitude": -78.75,
                   "longitude": 20.4
               }}
    customer = {"name": "John Doe", "id": 1}
    order_details = {"id": 1, "customer": customer}
    order = {"order_details": order_details, "product": product}

    schema = _register_referenced_schemas(sr, load_file)

    value_serializer = JSONSerializer(schema, sr)
    value_deserializer = JSONDeserializer(schema, schema_registry_client=sr)

    producer = kafka_cluster.producer(value_serializer=value_serializer)
    producer.produce(topic, value=order, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert all([actual[k] == v for k, v in order.items()])


def test_json_reference_custom(kafka_cluster, load_file):
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry()

    product = _TestProduct(product_id=1,
                           name="The ice sculpture",
                           price=12.50,
                           tags=["cold", "ice"],
                           dimensions={"length": 7.0,
                                       "width": 12.0,
                                       "height": 9.5},
                           location={"latitude": -78.75,
                                     "longitude": 20.4})
    customer = _TestCustomer(name="John Doe", id=1)
    order_details = _TestOrderDetails(id=1, customer=customer)
    order = _TestOrder(order_details=order_details, product=product)

    schema = _register_referenced_schemas(sr, load_file)

    value_serializer = JSONSerializer(schema, sr, to_dict=_testOrder_to_dict)
    value_deserializer = JSONDeserializer(schema, schema_registry_client=sr, from_dict=_testOrder_from_dict)

    producer = kafka_cluster.producer(value_serializer=value_serializer)
    producer.produce(topic, value=order, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert actual == order
