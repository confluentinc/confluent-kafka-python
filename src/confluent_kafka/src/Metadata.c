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


static PyObject *c_broker_to_py(Handle *self, PyObject *BrokerMetadata_type,
                                const rd_kafka_metadata_broker_t c_broker) {
        PyObject *broker;
        PyObject *key;

        broker = PyObject_CallObject(BrokerMetadata_type, NULL);
        if (!broker)
                return NULL;

        key = cfl_PyInt_FromInt(c_broker.id);

        if (PyObject_SetAttrString(broker, "id", key) == -1) {
                Py_DECREF(key);
                Py_DECREF(broker);
                return NULL;
        }
        Py_DECREF(key);

        if (cfl_PyObject_SetString(broker, "host",
                                        c_broker.host) == -1) {
                Py_DECREF(broker);
                return NULL;
        }
        if (cfl_PyObject_SetInt(broker, "port",
                                (int)c_broker.port) == -1) {
                Py_DECREF(broker);
                return NULL;
        }
        return broker;
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

                broker = c_broker_to_py(self, BrokerMetadata_type, c_brokers[i]);
                if (!broker)
                        goto err;

                key = cfl_PyInt_FromInt(c_brokers[i].id);

                if (PyDict_SetItem(dict, key, broker) == -1) {
                        Py_DECREF(key);
                        Py_DECREF(broker);
                        goto err;
                }

                Py_DECREF(broker);
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
        " Request metadata from the cluster.\n"
        " This method provides the same information as "
        " listTopics(), describeTopics() and describeCluster() in "
        " the Java Admin client.\n"
        "\n"
        " :param str topic: If specified, only request information about this topic, else return results for all topics in cluster. Warning: If auto.create.topics.enable is set to true on the broker and an unknown topic is specified, it will be created.\n"
        " :param float timeout: The maximum response time before timing out, or -1 for infinite timeout.\n"
        " :rtype: ClusterMetadata\n"
        " :raises: KafkaException\n");


static PyObject *
c_group_members_to_py(Handle *self, const struct rd_kafka_group_member_info *c_members,
                      int member_cnt) {
        PyObject *GroupMember_type, *list;
        int i;

        GroupMember_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                               "GroupMember");
        if (!GroupMember_type)
                return NULL;

        list = PyList_New(member_cnt);
        if (!list)
                goto err;

        for (i = 0; i < member_cnt; i++) {
                PyObject *member, *metadata, *assignment, *assignment_topic_partitions;

                member = PyObject_CallObject(GroupMember_type, NULL);
                if (!member)
                        goto err;

                if (cfl_PyObject_SetString(member, "id", c_members[i].member_id) == -1) {
                        goto err;
                }

                if (cfl_PyObject_SetString(member, "client_id", c_members[i].client_id) == -1) {
                        goto err;
                }

                if (cfl_PyObject_SetString(member, "client_host", c_members[i].client_host) == -1) {
                        goto err;
                }

                metadata = PyBytes_FromStringAndSize(c_members[i].member_metadata,
                                                     c_members[i].member_metadata_size);
                if (!metadata)
                        goto err;

                if (PyObject_SetAttrString(member, "metadata", metadata) == -1) {
                        Py_DECREF(metadata);
                        goto err;
                }
                Py_DECREF(metadata);

                assignment = PyBytes_FromStringAndSize(c_members[i].member_assignment,
                                                       c_members[i].member_assignment_size);
                if (!assignment)
                        goto err;

                if (PyObject_SetAttrString(member, "assignment", assignment) == -1) {
                        Py_DECREF(assignment);
                        goto err;
                }
                Py_DECREF(assignment);

                assignment_topic_partitions = c_parts_to_py(c_members[i].member_assignment_toppars);
                if (PyObject_SetAttrString(member, 
                                           "assignment_topic_partitions", 
                                           assignment_topic_partitions) == -1) {
                        Py_DECREF(assignment_topic_partitions);
                        goto err;
                }
                Py_DECREF(assignment_topic_partitions);

                PyList_SET_ITEM(list, i, member);
        }
        Py_DECREF(GroupMember_type);
        return list;
err:
        Py_DECREF(GroupMember_type);
        return NULL;
}


/**
 * @returns a GroupMetadata object populated with all metadata information
 *          from \p metadata, or NULL on error in which case an exception
 *          has been raised.
 */
static PyObject *
c_groups_to_py (Handle *self, const struct rd_kafka_group_list *group_list) {
        PyObject *GroupMetadata_type, *BrokerMetadata_type;
        PyObject *groups;
        int i;

        GroupMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                 "GroupMetadata");
        if (!GroupMetadata_type)
                return NULL;

        BrokerMetadata_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "BrokerMetadata");
        if (!BrokerMetadata_type) {
                Py_DECREF(GroupMetadata_type);
                return NULL;
        }

        groups = PyList_New(group_list->group_cnt);
        if (!groups)
                goto err;
        for (i = 0; i < group_list->group_cnt; i++) {
                PyObject *group, *error, *broker, *members, *py_is_simple_consumer_group;

                group = PyObject_CallObject(GroupMetadata_type, NULL);
                if (!group)
                        goto err;

                if (cfl_PyObject_SetString(group, "id",
                                           group_list->groups[i].group) == -1)
                        goto err;

                error = KafkaError_new_or_None(group_list->groups[i].err, NULL);

                if (PyObject_SetAttrString(group, "error", error) == -1) {
                        Py_DECREF(error);
                        goto err;
                }

                Py_DECREF(error);

                py_is_simple_consumer_group = PyBool_FromLong(group_list->groups[i].is_simple_consumer_group);
                if(PyObject_SetAttrString(group, "is_simple_consumer_group", py_is_simple_consumer_group) == -1) {
                        Py_DECREF(py_is_simple_consumer_group);
                        goto err;
                }
                Py_DECREF(py_is_simple_consumer_group);

                if (cfl_PyObject_SetString(group, "state",
                                           group_list->groups[i].state) == -1)
                        goto err;

                if (cfl_PyObject_SetString(group, "protocol_type",
                                           group_list->groups[i].protocol_type) == -1)
                        goto err;

                if (cfl_PyObject_SetString(group, "protocol",
                                           group_list->groups[i].protocol) == -1)
                        goto err;

                broker = c_broker_to_py(self, BrokerMetadata_type, group_list->groups[i].broker);
                if (!broker)
                        goto err;
                if (PyObject_SetAttrString(group, "broker", broker) == -1) {
                        Py_DECREF(broker);
                        goto err;
                }
                Py_DECREF(broker);

                members = c_group_members_to_py(self, group_list->groups[i].members,
                                                group_list->groups[i].member_cnt);
                if (!members)
                        goto err;
                if (PyObject_SetAttrString(group, "members", members) == -1) {
                        Py_DECREF(members);
                        goto err;
                }
                Py_DECREF(members);

                PyList_SET_ITEM(groups, i, group);
        }
        Py_DECREF(GroupMetadata_type);
        Py_DECREF(BrokerMetadata_type);
        return groups;
err:
        Py_DECREF(GroupMetadata_type);
        Py_DECREF(BrokerMetadata_type);
        Py_XDECREF(groups);
        return NULL;
}


/**
 * @returns a GroupMetadata object populated with all metadata information
 *          from \p metadata, or NULL on error in which case an exception
 *          has been raised.
 */
static PyObject *
c_groups_to_consumer_group_listing_py (Handle *self, const struct rd_kafka_group_list *group_list) {
        PyObject *ConsumerGroupListing_type = NULL;
        PyObject *groups = NULL;
        int i;

        ConsumerGroupListing_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                        "ConsumerGroupListing");
        if (!ConsumerGroupListing_type) {
                goto err;
        }

        groups = PyList_New(group_list->group_cnt);
        if (!groups) {
                goto err;
        }

        for (i = 0; i < group_list->group_cnt; i++) {

                PyObject *args, *kwargs;
                PyObject *consumer_group_listing;
                PyObject *error = NULL;
                PyObject *py_is_simple_consumer_group = NULL;
                const struct rd_kafka_group_info c_current_consumer_group = group_list->groups[i];

                kwargs = PyDict_New();

                cfl_PyDict_SetString(kwargs, "group_id", c_current_consumer_group.group);

                py_is_simple_consumer_group = PyBool_FromLong(c_current_consumer_group.is_simple_consumer_group);
                if(PyDict_SetItemString(kwargs, "is_simple_consumer_group", py_is_simple_consumer_group) == -1) {
                        Py_DECREF(py_is_simple_consumer_group);
                        goto err;
                }
                Py_DECREF(py_is_simple_consumer_group);

                cfl_PyDict_SetInt(kwargs, "state", c_current_consumer_group.state_code);

                error = KafkaError_new_or_None(c_current_consumer_group.err, NULL);
                if(PyDict_SetItemString(kwargs, "error", error) == -1) {
                        Py_DECREF(error);
                        goto err;
                }
                Py_DECREF(error);

                args = PyTuple_New(0);
                consumer_group_listing = PyObject_Call(ConsumerGroupListing_type, args, kwargs);

                Py_DECREF(args);
                Py_DECREF(kwargs);
                PyList_SET_ITEM(groups, i, consumer_group_listing);
        }
        Py_DECREF(ConsumerGroupListing_type);
        return groups;
err:
        Py_XDECREF(ConsumerGroupListing_type);
        Py_XDECREF(groups);
        return NULL;
}


/**
 * @brief Older Method for describing consumer groups
 * 
 * TODO: Deprecation Note
 */
PyObject *
list_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        CallState cs;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;
        const struct rd_kafka_group_list *group_list = NULL;
        const char *group = NULL;
        double tmout = -1.0f;
        static char *kws[] = {"group", "timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zd", kws,
                                         &group, &tmout))
                return NULL;

        CallState_begin(self, &cs);

        err = rd_kafka_list_groups(self->rk, group, &group_list,
                                   cfl_timeout_ms(tmout));

        if (!CallState_end(self, &cs)) {
                /* Exception raised */
                goto end;
        }

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                cfl_PyErr_Format(err,
                                 "Failed to list groups: %s",
                                 rd_kafka_err2str(err));

                goto end;
        }
        result = c_groups_to_py(self, group_list);
end:
        if (group_list != NULL) {
                rd_kafka_group_list_destroy(group_list);
        }
        return result;
}


const char list_groups_doc[] = PyDoc_STR(
        ".. py:function:: list_groups([group=None], [timeout=-1])\n"
        "\n"
        " Request Group Metadata from cluster.\n"
        " This method provides the same information as"
        " listGroups(), describeGroups() in the Java Admin client.\n"
        "\n"
        " :param str group: If specified, only request info about this group, else return for all groups in cluster"
        " :param float timeout: Maximum response time before timing out, or -1 for infinite timeout.\n"
        " :rtype: GroupMetadata\n"
        " :raises: KafkaException\n");

/**
 * @brief Converts list consumer groups Python options to C options
 * 
 * @param options Python options
 * @param c_options Pointer to the C options
 * @return 1 for success and 0 for failure
 */
int list_consumer_groups_options_py_to_c(PyObject *states_int, double request_timeout,
                                      rd_kafka_list_consumer_groups_options_t **c_options) {

        rd_kafka_consumer_group_state_t *c_states = NULL;
        int c_states_cnt = 0;

        if(states_int != NULL) {

                if (!PyList_Check(states_int)) {
                        PyErr_SetString(PyExc_TypeError,
                                        "'states_int' property of options should be of list type");
                        goto err;
                }

                c_states_cnt = (size_t) PyList_Size(states_int);
                c_states = malloc(c_states_cnt*sizeof(rd_kafka_consumer_group_state_t));
                int i;

                for(i = 0 ; i < c_states_cnt ; i++) {
                        PyObject *state_int = PyList_GET_ITEM(states_int, i);
                        if(!PyLong_Check(state_int)) {
                                PyErr_SetString(PyExc_TypeError,
                                                "'states_int' should only contain integer values");
                                goto err;
                        }
                        /**
                         * TODO: Assuming that the state will be in int range as we are 
                         * the one generating states_int in the first place 
                         * or Shall I verify it in the range using RD_KAFKA_CGRP_STATE__CNT?
                         */
                        c_states[i] = (rd_kafka_consumer_group_state_t) PyLong_AS_LONG(state_int);
                }

        }

        *c_options = rd_kafka_list_consumer_groups_options_new(cfl_timeout_ms(request_timeout), 
                                                               c_states, 
                                                               c_states_cnt);
        free(c_states);
        return 1;

err:
        if(c_states != NULL) {
                free(c_states);
        }
        return 0;
}

/**
 * @brief List Consumer Group
 * 
 * TODO: Proper doc
 */
PyObject *
list_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        CallState cs;
        PyObject *result = NULL;
        PyObject *states_int = NULL;
        rd_kafka_list_consumer_groups_options_t *c_options = NULL;

        rd_kafka_resp_err_t err;
        const struct rd_kafka_group_list *group_list = NULL;
        double request_timeout = -1.0f;
        static char *kws[] = {"states_int", "request_timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|Od", kws, &states_int, &request_timeout)) {
                return NULL;
        }

        if(!list_consumer_groups_options_py_to_c(states_int, request_timeout, &c_options)) {
                goto end;
        }

        CallState_begin(self, &cs);

        err = rd_kafka_list_consumer_groups(self->rk, &group_list, c_options);

        if (!CallState_end(self, &cs)) {
                /* Exception raised */
                goto end;
        }

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                cfl_PyErr_Format(err,
                                 "Failed to list groups: %s",
                                 rd_kafka_err2str(err));

                goto end;
        }
        result = c_groups_to_consumer_group_listing_py(self, group_list);
end:
        if (group_list != NULL) {
                rd_kafka_group_list_destroy(group_list);
        }
        if(c_options != NULL) {

                rd_kafka_list_consumer_groups_options_destroy(c_options);
        }
        return result;
}


const char list_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: list_consumer_groups([timeout=-1])\n"
        "\n"
        " Request Groups from the cluster.\n"
        " This method provides the same information as"
        " listConsumerGroups() in the Java Admin client.\n"
        "\n"
        " :param float timeout: Maximum response time (approx.) before timing out, or -1 for infinite timeout.\n"
        " :rtype: GroupMetadata\n"
        " :raises: KafkaException\n");


/**
 * @brief Older Method for describing consumer groups
 * 
 * TODO: Deprecation Note
 */
PyObject *
describe_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        CallState cs;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;
        const struct rd_kafka_group_list *group_list = NULL;
        PyObject *groups;
        int c_groups_cnt;
        int i;
        const char **c_groups = NULL;
        double tmout = -1.0f;
        static char *kws[] = {"group_ids", "request_timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|d", kws,
                                         &groups, &tmout))
                return NULL;

        rd_kafka_describe_consumer_groups_options_t *c_options = 
                rd_kafka_describe_consumer_groups_options_new(cfl_timeout_ms(tmout));

        if (!PyList_Check(groups) || (c_groups_cnt = (int)PyList_Size(groups)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of groups objects");
                return NULL;
        }

        c_groups = malloc(sizeof(*c_groups)*c_groups_cnt);

        for(i = 0 ; i < c_groups_cnt; i++) {
                PyObject *group = PyList_GET_ITEM(groups, i);
                PyObject *ugroup;
                PyObject *uogroup = NULL;
                if (group == Py_None ||
                    !(ugroup = cfl_PyObject_Unistr(group))) {
                        PyErr_Format(PyExc_ValueError,
                                     "Expected list of group strings, "
                                     "not %s",
                                     ((PyTypeObject *)PyObject_Type(group))->
                                     tp_name);
                        goto end;
                }
                c_groups[i] = cfl_PyUnistr_AsUTF8(ugroup, &uogroup);

                Py_XDECREF(ugroup);
                Py_XDECREF(uogroup);
        }

        CallState_begin(self, &cs);

        err = rd_kafka_describe_consumer_groups(self->rk, c_groups, c_groups_cnt, &group_list, c_options);

        if (!CallState_end(self, &cs)) {
                /* Exception raised */
                goto end;
        }

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                cfl_PyErr_Format(err,
                                 "Failed to list groups: %s",
                                 rd_kafka_err2str(err));

                goto end;
        }
        result = c_groups_to_py(self, group_list);
end:
        if (group_list != NULL) {
                rd_kafka_group_list_destroy(group_list);
        }
        if(c_groups != NULL) {
                free(c_groups);
        }
        return result;
}

/** 
 * TODO: Write Doc
 */
const char describe_consumer_groups_doc[] = PyDoc_STR("Improve");
