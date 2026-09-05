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

# This uses custom OAuth callback function oauth_callback to obtain the token via 
# DefaultAzureCredential from azure.identity. 
# Callback function, beside token value and expiration time, must also return: 
# extensions: confluent cluster id (logicalCluster) , identity pool id (identityPoolId) and principal.

import logging
import functools
import argparse
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from azure.identity import DefaultAzureCredential


def oauth_callback(args, config):
    """
    This is the custom callback function that retrieves an OAuth token
    using Azure's DefaultAzureCredential. It's called by librdkafka
    whenever a new token is needed.
    """
  
    # Define the scope for the token.
    # This is the resource identifier for your Azure AD app registration.
    # The /.default scope requests a token with all delegated permissions.
    scope = ' '.join(args.scopes) 

    # Get a credential instance. DefaultAzureCredential automatically finds
    # the right credential (e.g., Managed Identity, Azure CLI, etc.).
    credential = DefaultAzureCredential()
    
    # Request the token. The get_token() method returns a TokenCredential object
    # with the token string and its expiry time.
    token_object = credential.get_token(scope)
    
    # Extract the token string and expiry time (in milliseconds since epoch)
    # Note: The 'expires_on' property is a timestamp in seconds; convert to milliseconds.
    token_value = token_object.token
    expiry_time_ms = int(token_object.expires_on * 1000)
    
    # Set extensions, logicalCluster and identity pool id
    ext = {"logicalCluster": args.logical_cluster , "identityPoolId": args.identity_pool_id}
    
    # Principal can be empty
    principal = ""

    # The callback must return a tuple with a token and expiry time.
    return (token_value, expiry_time_ms, principal, ext)



def producer_config(args):
    """
    Configures the Kafka producer with the necessary SASL/OAUTHBEARER settings.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    params = {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': functools.partial(oauth_callback, args),
        #'debug': 'all'
    } 
    return params


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    """
    if err is not None:
        print('Delivery failed for User record {}: {}'.format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    """
    The main function to create the producer and start producing messages.
    """
    topic = args.topic
    delimiter = args.delimiter
    producer_conf = producer_config(args)
    producer = Producer(producer_conf)
    serializer = StringSerializer('utf_8')

    print('Producing records to topic {}. ^C to exit.'.format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        
        try:
            msg_data = input(">")
            msg = msg_data.split(delimiter)
            
            if len(msg) == 2:
                producer.produce(topic=topic,
                                 key=serializer(msg[0]),
                                 value=serializer(msg[1]),
                                 on_delivery=delivery_report)
            else:
                producer.produce(topic=topic,
                                 value=serializer(msg[0]),
                                 on_delivery=delivery_report)
        except KeyboardInterrupt:
            break

    print('\nFlushing {} records...'.format(len(producer)))
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="OAUTH example with Azure Managed Identity")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_oauth",
                        help="Topic name")
    parser.add_argument('-d', dest="delimiter", default="|",
                        help="Key-Value delimiter. Defaults to '|'")
    parser.add_argument('--scopes', dest="scopes", required=True, nargs='+',
                        help="Scopes requested from OAuth server.")
    parser.add_argument('--logical-cluster', dest="logical_cluster", required=False, help="Logical Cluster.")
    parser.add_argument('--identity-pool-id', dest="identity_pool_id", required=False, help="Identity Pool ID.")


    main(parser.parse_args())
