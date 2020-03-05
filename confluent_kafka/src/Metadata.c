/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "confluent_kafka.h"


/**
 * @name Cluster and topic metadata retrieval
 *
 */


/**
 * @returns a dict<partition_id, PartitionMetadata>,
 *          or NULL (and exception) on error.
 */
static PyObject *
c_partitions_to_py (Handle *self,
                    const rd_kafka_metadata_partition_t *c_partitions,
                    int partition_cnt) {
        PyObject *PartitionMetadata_type;
        PyObject *dict;
        int i;

        PartitionMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                     "PartitionMetadata");
        if (!PartitionMetadata_type)
                return NULL;

        dict = PyDict_New();
        if (!dict)
                goto err;

        for (i = 0 ; i < partition_cnt ; i++) {
                PyObject *partition, *key;
                PyObject *error, *replicas, *isrs;

                partition = PyObject_CallObject(PartitionMetadata_type, NULL);
                if (!partition)
                        goto err;

                key = cfl_PyInt_FromInt(c_partitions[i].id);

                if (PyDict_SetItem(dict, key, partition) == -1) {
                        Py_DECREF(key);
                        Py_DECREF(partition);
                        goto err;
                }

                Py_DECREF(key);
                Py_DECREF(partition);

                if (cfl_PyObject_SetInt(partition, "id",
                                        (int)c_partitions[i].id) == -1)
                        goto err;
                if (cfl_PyObject_SetInt(partition, "leader",
                                        (int)c_partitions[i].leader) == -1)
                        goto err;

                error = KafkaError_new_or_None(c_partitions[i].err, NULL);

                if (PyObject_SetAttrString(partition, "error", error) == -1) {
                        Py_DECREF(error);
                        goto err;
                }

                Py_DECREF(error);

                /* replicas */
                replicas = cfl_int32_array_to_py_list(
                        c_partitions[i].replicas,
                        (size_t)c_partitions[i].replica_cnt);
                if (!replicas)
                        goto err;

                if (PyObject_SetAttrString(partition, "replicas",
                                           replicas) == -1) {
                        Py_DECREF(replicas);
                        goto err;
                }
                Py_DECREF(replicas);

                /* isrs */
                isrs = cfl_int32_array_to_py_list(
                        c_partitions[i].isrs, (size_t)c_partitions[i].isr_cnt);
                if (!isrs)
                        goto err;

                if (PyObject_SetAttrString(partition, "isrs", isrs) == -1) {
                        Py_DECREF(isrs);
                        goto err;
                }
                Py_DECREF(isrs);
        }

        Py_DECREF(PartitionMetadata_type);
        return dict;

 err:
        Py_DECREF(PartitionMetadata_type);
        Py_XDECREF(dict);
        return NULL;
}


/**
 * @returns a dict<topic, TopicMetadata>, or NULL (and exception) on error.
 */
static PyObject *
c_topics_to_py (Handle *self, const rd_kafka_metadata_topic_t *c_topics,
                int topic_cnt) {
        PyObject *TopicMetadata_type;
        PyObject *dict;
        int i;

        TopicMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "TopicMetadata");
        if (!TopicMetadata_type)
                return NULL;

        dict = PyDict_New();
        if (!dict)
                goto err;

        for (i = 0 ; i < topic_cnt ; i++) {
                PyObject *topic;
                PyObject *error, *partitions;

                topic = PyObject_CallObject(TopicMetadata_type, NULL);
                if (!topic)
                        goto err;

                if (PyDict_SetItemString(dict, c_topics[i].topic,
                                         topic) == -1) {
                        Py_DECREF(topic);
                        goto err;
                }

                Py_DECREF(topic);

                if (cfl_PyObject_SetString(topic, "topic",
                                           c_topics[i].topic) == -1)
                        goto err;

                error = KafkaError_new_or_None(c_topics[i].err, NULL);

                if (PyObject_SetAttrString(topic, "error", error) == -1) {
                        Py_DECREF(error);
                        goto err;
                }

                Py_DECREF(error);

                /* partitions dict */
                partitions = c_partitions_to_py(self,
                                                c_topics[i].partitions,
                                                c_topics[i].partition_cnt);
                if (!partitions)
                        goto err;

                if (PyObject_SetAttrString(topic, "partitions",
                                           partitions) == -1) {
                        Py_DECREF(partitions);
                        goto err;
                }

                Py_DECREF(partitions);
        }

        Py_DECREF(TopicMetadata_type);
        return dict;

 err:
        Py_DECREF(TopicMetadata_type);
        Py_XDECREF(dict);
        return NULL;
}


/**
 * @returns a dict<broker_id, BrokerMetadata>, or NULL (and exception) on error.
 */
static PyObject *c_brokers_to_py (Handle *self,
                                  const rd_kafka_metadata_broker_t *c_brokers,
                                  int broker_cnt) {
        PyObject *BrokerMetadata_type;
        PyObject *dict;
        int i;

        BrokerMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "BrokerMetadata");
        if (!BrokerMetadata_type)
                return NULL;

        dict = PyDict_New();
        if (!dict)
                goto err;

        for (i = 0 ; i < broker_cnt ; i++) {
                PyObject *broker;
                PyObject *key;

                broker = PyObject_CallObject(BrokerMetadata_type, NULL);
                if (!broker)
                        goto err;

                key = cfl_PyInt_FromInt(c_brokers[i].id);

                if (PyDict_SetItem(dict, key, broker) == -1) {
                        Py_DECREF(key);
                        Py_DECREF(broker);
                        goto err;
                }

                Py_DECREF(broker);

                if (PyObject_SetAttrString(broker, "id", key) == -1) {
                        Py_DECREF(key);
                        goto err;
                }
                Py_DECREF(key);

                if (cfl_PyObject_SetString(broker, "host",
                                           c_brokers[i].host) == -1)
                        goto err;
                if (cfl_PyObject_SetInt(broker, "port",
                                        (int)c_brokers[i].port) == -1)
                        goto err;
        }

        Py_DECREF(BrokerMetadata_type);
        return dict;

 err:
        Py_DECREF(BrokerMetadata_type);
        Py_XDECREF(dict);
        return NULL;
}


/**
 * @returns a ClusterMetadata object populated with all metadata information
 *          from \p metadata, or NULL on error in which case an exception
 *          has been raised.
 */
static PyObject *
c_metadata_to_py (Handle *self, const rd_kafka_metadata_t *metadata) {
        PyObject *ClusterMetadata_type;
        PyObject *cluster = NULL, *brokers, *topics;
#if RD_KAFKA_VERSION >= 0x000b0500
        char *cluster_id;
#endif

        ClusterMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                   "ClusterMetadata");
        if (!ClusterMetadata_type)
                return NULL;

        cluster = PyObject_CallObject(ClusterMetadata_type, NULL);
        Py_DECREF(ClusterMetadata_type);

        if (!cluster)
                return NULL;

#if RD_KAFKA_VERSION >= 0x000b0500
        if (cfl_PyObject_SetInt(
                    cluster, "controller_id",
                    (int)rd_kafka_controllerid(self->rk, 0)) == -1)
                goto err;

        if ((cluster_id = rd_kafka_clusterid(self->rk, 0))) {
                if (cfl_PyObject_SetString(cluster, "cluster_id",
                                           cluster_id) == -1) {
                        free(cluster_id);
                        goto err;
                }

                free(cluster_id);
        }
#endif

        if (cfl_PyObject_SetInt(cluster, "orig_broker_id",
                                (int)metadata->orig_broker_id) == -1)
                goto err;

        if (metadata->orig_broker_name &&
            cfl_PyObject_SetString(cluster, "orig_broker_name",
                                   metadata->orig_broker_name) == -1)
                goto err;



        /* Create and set 'brokers' dict */
        brokers = c_brokers_to_py(self,
                                  metadata->brokers,
                                  metadata->broker_cnt);
        if (!brokers)
                goto err;

        if (PyObject_SetAttrString(cluster, "brokers", brokers) == -1) {
                Py_DECREF(brokers);
                goto err;
        }
        Py_DECREF(brokers);

        /* Create and set 'topics' dict */
        topics = c_topics_to_py(self, metadata->topics, metadata->topic_cnt);
        if (!topics)
                goto err;

        if (PyObject_SetAttrString(cluster, "topics", topics) == -1) {
                Py_DECREF(topics);
                goto err;
        }
        Py_DECREF(topics);

        return cluster;

 err:
        Py_XDECREF(cluster);
        return NULL;
}


PyObject *
list_topics (Handle *self, PyObject *args, PyObject *kwargs) {
        CallState cs;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;
        const rd_kafka_metadata_t *metadata = NULL;
        rd_kafka_topic_t *only_rkt = NULL;
        const char *topic = NULL;
        double tmout = -1.0f;
        static char *kws[] = {"topic", "timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zd", kws,
                                         &topic, &tmout))
                return NULL;

        if (topic != NULL) {
                if (!(only_rkt = rd_kafka_topic_new(self->rk,
                                                    topic, NULL))) {
                        return PyErr_Format(
                                PyExc_RuntimeError,
                                "Unable to create topic object "
                                "for \"%s\": %s", topic,
                                rd_kafka_err2str(rd_kafka_last_error()));
                }
        }

        CallState_begin(self, &cs);

        err = rd_kafka_metadata(self->rk, !only_rkt, only_rkt, &metadata,
                                cfl_timeout_ms(tmout));

        if (!CallState_end(self, &cs)) {
                /* Exception raised */
                goto end;
        }

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                cfl_PyErr_Format(err,
                                 "Failed to get metadata: %s",
                                 rd_kafka_err2str(err));

                goto end;
        }

        result = c_metadata_to_py(self, metadata);

 end:
        if (metadata != NULL) {
                rd_kafka_metadata_destroy(metadata);
        }

        if (only_rkt != NULL) {
                rd_kafka_topic_destroy(only_rkt);
        }

        return result;
}

const char list_topics_doc[] = PyDoc_STR(
        ".. py:function:: list_topics([topic=None], [timeout=-1])\n"
        "\n"
        " Request Metadata from cluster.\n"
        " This method provides the same information as "
        " listTopics(), describeTopics() and describeCluster() in "
        " the Java Admin client.\n"
        "\n"
        " :param str topic: If specified, only request info about this topic, else return for all topics in cluster. Warning: If auto.create.topics.enable is set to true on the broker and an unknown topic is specified it will be created.\n"
        " :param float timeout: Maximum response time before timing out, or -1 for infinite timeout.\n"
        " :rtype: ClusterMetadata \n"
        " :raises: KafkaException \n");
