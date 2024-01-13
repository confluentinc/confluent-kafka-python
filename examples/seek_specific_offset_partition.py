#!/usr/bin/env python3
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
# A simple example demonstrating use of JSONDeserializer.
#


import logging
import argparse
from confluent_kafka import Consumer,TopicPartition
from confluent_kafka.error import KafkaException


# TODO: MAX OFFSET TO EXTRACT AT A TIME

 
def fetch_partition_list(kafka_consumer:Consumer, topic:str):
    """
    Parameters
    ----------
    kafka_consumer : kafka_consumer object of cimpl module
        defined consumer for ex: kafka_consumer = kafka_consumer({'bootstrap.servers': "servers"})
    topic : str
        Name of the topic on Kafka Server to subscribe to.
        Ex: "Food"
        
    Returns
    -------
    partition_list : list
        list of partitions in the topic.
    topic_partition: list
        list of topic partition objects (to iterate or multi-thread over)
    partition_offset: list
        list of list having partition,[minimum, max offset] of each partition
    
    Operation
    ----------
    c1=kafka_consumer.list_topics() #Admin Cluster metadata
    c2=kafka_consumer.list_topics().topics #Dictionary with "topic_name":topic_matadata as key value pair
    c3=kafka_consumer.list_topics().topics.get(topic) # Get Specific "topic" metadata
    c4=kafka_consumer.list_topics().topics.get(topic).partitions # Dictionary with "partition":"partition_metadata" as key value pair
    c5=list(kafka_consumer.list_topics().topics.get(topic).partitions.keys()) #List all keys (partitions) of above dictionary
    
    """
    try:
        kafka_consumer.subscribe([topic])
    except KafkaException as exc:
        logging.error("Unable to subscribe to topic {}.\n Exception: {}".format(str(topic),str(exc)))
        return [],[],[]
        
    try:
        partition_list=list(kafka_consumer.list_topics().topics.get(topic).partitions.keys())
        topic_partition=[TopicPartition(topic,partition) for partition in partition_list]
        
        partition_offset=[[ele1,(kafka_consumer.get_watermark_offsets(ele2))] for ele1,ele2
            in zip(partition_list,topic_partition)]
    except KafkaException as exc:
        logging.error("Unable to extract offset list from topic {}.\n Exception: {}".format(str(topic),str(exc)))
        return [],[],[]
    return partition_list,topic_partition,partition_offset




def fetch_message_partition(consumer_conf,topic,partition,min_offset,max_offset):
    """
    Returns list of messages and [offset,partition] for min,max offsets for a given partition and topic. 

    Parameters
    ----------
    consumer_conf: json
        Kafka server Configurations to be used for creating consumer/producer object.
    topic : str,
        topic of Kafka server from which messages to be consumed via consumer.
    partition : int
        Partition of topic for which data is to be fetched.
    min_offset : int
        Start offset from which data is to be fetched from Partition (if available). 
        Else from lowest available offset of the partition.
    max_offset : int
        End offset till which data is to be fetched from Partition (if available). 
        Else till highest available offset of the partition.
    

    Returns
    -------
    partition : TYPE
        DESCRIPTION.
    TYPE
        DESCRIPTION.
    TYPE
        DESCRIPTION.

    """
    total_messages=[]
    total_partition_offest=[]
    consumer = Consumer(consumer_conf)
    available_min_offset,available_max_offset=consumer.get_watermark_offsets(TopicPartition(topic,partition))
    
    #Check availabel min and max offset for the partition of the topic
    if min_offset>max_offset:
        logging.info("Provided minimum offset: {} greater than Provided max offset:{} in partition:{} Topic:{}".format(str(min_offset),
                      str(max_offset),str(partition),str(topic)))
        return [],[]
    if  min_offset< available_min_offset:
        logging.info("Minimum Offset: {} less than available minimum offset: {} in partition:{} Topic:{}".format(str(min_offset),
                      str(available_min_offset),str(partition),str(topic)))
        min_offset=available_min_offset
    if  max_offset> available_max_offset:
        logging.info("Maximum Offset: {} greater than available maximum offset: {} in partition:{} Topic:{}".format(str(max_offset),
                      str(available_max_offset),str(partition),str(topic)))
        max_offset=available_max_offset
    
    #Seeking the pointer to set to read message from the min offset
    try:
        partition1=TopicPartition(topic,partition,min_offset)
        consumer.assign([partition1])
        consumer.seek( partition1)
        start_offset=min_offset
    except Exception as exc:
        logging.error("Unable to seek consumer to Topic:{} Partition:{} and Offset:{}.\nException:{}".format(
            str(topic),str(partition),str(min_offset),str(exc)))
    
    # Reading only the 1st message along with offset
    try:
        message=None
        while message==None:
            message=consumer.poll()
            start_offset=message.offset()
            break
        total_messages.append(str(message.value()))
        total_partition_offest.append([message.offset(),message.partition()])
    # Reading the messages after the 1st message
        while start_offset<max_offset:
            message = consumer.poll()
            if message !=None:
                total_messages.append(str(message.value()))
                total_partition_offest.append([message.offset(),message.partition()])
                
                start_offset=message.offset()                    
        consumer.pause([partition1])

    except Exception as exc:
        logging.fatal("Unable to Fetch Data from Topic:{} Partition:{} . Exception:{} ".format(
            str(topic),str(partition),str(exc)))
    return total_messages,total_partition_offest

def main(args):
    topic=args.topic
    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}
    partition=args.partition
    start_offset=args.start_offset
    end_offset=args.end_offset
    
    try:
        consumer = Consumer(consumer_conf)
    except KafkaException as exc:
        logging.error("Unable to connect to Kafka Server.\n Exception:{} "+str(exc))
    
    
    partition_list,topic_partition,partition_offset= fetch_partition_list(consumer, topic)
    
    #Fetching Data From Specific Partition.
    # Data across all partitions for a given topic can similarly be fetched via for loop or multithreading across partitions.
    if partition in partition_list:
        total_messages,total_partition_offest=fetch_message_partition(partition,start_offset,end_offset,topic="my")
        if len(total_messages)>0:
            total_messages_output=[[ele1,ele2[0],ele2[1]] for ele1,ele2 in zip(total_messages,total_partition_offest)]
            # Saving the message along with offset and partition as txt file
            with open("file.txt", "w") as output:
                output.write(str(total_messages_output))
    else:
        logging.error("Partition {} not in consumer."+str(partition))

    consumer.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="JSONDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-g', dest="group", default="example_serde_json",
                        help="Consumer group")
    parser.add_argument('-t', dest="topic", default="topic_z",
                        help="Topic name")
    parser.add_argument('-p', dest="partition", default="partition",
                        help="Partition of topic to fetch data from")
    parser.add_argument('-sof', dest="start_offset", required=True,
                        help="Start Offset for Partition p")
    parser.add_argument('-eof', dest="end_offset", required=True,
                        help="End Offset for Partition p")
    
    
    



    main(parser.parse_args())


   
