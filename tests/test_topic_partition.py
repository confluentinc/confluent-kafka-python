#!/usr/bin/env python

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
