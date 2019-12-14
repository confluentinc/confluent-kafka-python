#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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
# limit
#


from confluent_kafka import TopicPartition


def test_sort():
    """ TopicPartition sorting (rich comparator) """

    # sorting uses the comparator
    correct = [TopicPartition('topic1', 3),
               TopicPartition('topic3', 0),
               TopicPartition('topicA', 5),
               TopicPartition('topicA', 5)]

    tps = sorted([TopicPartition('topicA', 5),
                  TopicPartition('topic3', 0),
                  TopicPartition('topicA', 5),
                  TopicPartition('topic1', 3)])

    assert correct == tps


def test_cmp():
    """ TopicPartition comparator """

    assert TopicPartition('aa', 19002) > TopicPartition('aa', 0)
    assert TopicPartition('aa', 13) >= TopicPartition('aa', 12)
    assert TopicPartition('BaB', 9) != TopicPartition('Card', 9)
    assert TopicPartition('b3x', 4) == TopicPartition('b3x', 4)
    assert TopicPartition('ulv', 2) < TopicPartition('xy', 0)
    assert TopicPartition('ulv', 2) <= TopicPartition('ulv', 3)


def test_hash():

    tp1 = TopicPartition('test', 99)
    tp2 = TopicPartition('somethingelse', 12)
    assert hash(tp1) != hash(tp2)


def test_subclassing():
    class SubTopicPartition(TopicPartition):
        def __init__(self, topic_part_str):
            topic, part = topic_part_str.split(":")
            super(SubTopicPartition, self).__init__(topic=topic, partition=int(part))

    st = SubTopicPartition("topic1:0")
    assert st.topic == "topic1"
    assert st.partition == 0

    st = SubTopicPartition("topic2:920")
    assert st.topic == "topic2"
    assert st.partition == 920
