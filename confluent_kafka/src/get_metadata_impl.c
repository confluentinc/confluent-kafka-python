/**
 * Copyright 2016 Confluent Inc.
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


static int
set_long_in_dict(PyObject *target, const char *key, const long val)
{
	int err;
	PyObject *py_val;

	if ((py_val = PyLong_FromLong(val)) == NULL) {
		return -1;
	}

	err =  PyDict_SetItemString(target, key, py_val);
	Py_DECREF(py_val);
	return err;
}


static int
set_char_in_dict(PyObject *target, const char *key, const char *val)
{
	int err;
	PyObject *py_val;

	py_val = cfl_PyUnistr(_FromString(val));

	if (py_val == NULL) {
		return -1;
	}

	err = PyDict_SetItemString(target, key, py_val);
	Py_DECREF(py_val);
	return err;
}


#define SET_LONG_IN_DICT(target, ptr, member) \
    set_long_in_dict(target, #member, (ptr)->member)


#define SET_CHAR_IN_DICT(target, ptr, member) \
    set_char_in_dict(target, #member, (ptr)->member)

struct broker_entry {
	int32_t id;
	PyObject *py_broker;
};

static PyObject *
find_broker(int32_t id, const struct broker_entry *brokers, int broker_cnt)
{
	int idx;

	for (idx = 0; idx < broker_cnt; idx++) {
		if (id == brokers[idx].id) {
			return brokers[idx].py_broker;
		}
	}

	return NULL;
}

static PyObject *
make_broker_list(const int32_t *ids, int id_cnt,
	       	 const struct broker_entry *brokers, int broker_cnt)
{
	PyObject *list;
	int idx;

	list = PyList_New(id_cnt);
	if (!list) {
		return NULL;
	}

	for (idx = 0; idx < id_cnt; idx++) {
		PyObject *broker = find_broker(ids[idx], brokers, broker_cnt);
		if (!broker) {
			goto error;
		}

		Py_INCREF(broker);
		PyList_SetItem(list, idx, broker);
	}

	return list;

error:
	Py_XDECREF(list);
	return NULL;
}

static PyObject *
make_partition(PyObject *py_topic,
               const rd_kafka_metadata_partition_t *partition,
	       const struct broker_entry *brokers, int broker_cnt)
{
	PyObject *py_part, *leader, *replica_list = NULL, *isr_list = NULL;
	int err;

	if ((py_part = PyDict_New()) == NULL) {
		goto error;
	}

	if (PyDict_SetItemString(py_part, "topic", py_topic) == -1) {
		goto error;
	}

	if (set_long_in_dict(py_part, "partition", partition->id) == -1) {
		goto error;
	}

	leader = find_broker(partition->leader, brokers, broker_cnt);
	if (!leader) {
		goto error;
	}

	err = PyDict_SetItemString(py_part, "leader", leader);
	if (err != 0) {
		goto error;
	}

	replica_list = make_broker_list(partition->replicas,
		       			partition->replica_cnt, brokers,
				       	broker_cnt);
	if (!replica_list) {
		goto error;
	}

	err = PyDict_SetItemString(py_part, "replicas", replica_list);
	if (err != 0) {
		goto error;
	}

	isr_list = make_broker_list(partition->isrs, partition->isr_cnt,
		       		    brokers, broker_cnt);
	if (!isr_list) {
		goto error;
	}

	err = PyDict_SetItemString(py_part, "inSyncReplicas", isr_list);
	if (err != 0) {
		goto error;
	}

	return py_part;

error:
	Py_XDECREF(py_part);
	Py_XDECREF(isr_list);
	Py_XDECREF(replica_list);
	return NULL;
}

static PyObject *
make_partition_list(const char *topic, int partition_cnt,
		    const rd_kafka_metadata_partition_t *partitions,
		    const struct broker_entry *brokers, int broker_cnt)
{
	int err;
	int idx;

	PyObject *py_partitions = NULL;
	PyObject *py_part = NULL;
	PyObject *py_topic = NULL;

	if ((py_partitions = PyList_New(partition_cnt)) == NULL) {
		goto error;
	}

	py_topic = cfl_PyUnistr(_FromString(topic));
	if (!py_topic) {
		goto error;
	}

	for (idx = 0; idx < partition_cnt; ++idx) {
		py_part = make_partition(py_topic, partitions + idx, brokers,
			       		 broker_cnt);
		if (!py_part) {
			goto error;
		}

		err = PyList_SetItem(py_partitions, idx, py_part);
		if (err == -1) {
			goto error;
		}
	}

	return py_partitions;

error:
	Py_XDECREF(py_partitions);
	Py_XDECREF(py_part);
	Py_XDECREF(py_topic);
	return NULL;
}

static PyObject *
topics_to_dict(int topic_cnt, const rd_kafka_metadata_topic_t *topics,
       	       const struct broker_entry *brokers, int broker_cnt)
{
	int idx;
	PyObject *py_topics = NULL;
	PyObject *py_partition_list = NULL;

	if ((py_topics = PyDict_New()) == NULL) {
		goto error;
	}

	for (idx=0; idx < topic_cnt; ++idx) {
		py_partition_list = make_partition_list(topics[idx].topic,
							topics[idx].partition_cnt,
							topics[idx].partitions,
						       	brokers, broker_cnt);

		if (py_partition_list == NULL) {
			goto error;
		}

		if (PyDict_SetItemString(py_topics, topics[idx].topic, py_partition_list) == -1) {
			goto error;
		}

		Py_DECREF(py_partition_list);
	}

	return py_topics;

error:
	Py_XDECREF(py_topics);
	Py_XDECREF(py_partition_list);
	return NULL;
}

static struct broker_entry *
make_broker_objects(int broker_cnt, const rd_kafka_metadata_broker_t *brokers)
{
	int idx;
	PyObject *py_broker = NULL;

	struct broker_entry *py_brokers = calloc(broker_cnt, sizeof(struct broker_entry));
	if (!py_brokers) {
		return NULL;
	}

	for (idx = 0; idx < broker_cnt; ++idx) {
		if ((py_broker = PyDict_New()) == NULL) {
			goto error;
		}

		if (SET_LONG_IN_DICT(py_broker, &brokers[idx], id) == -1) {
			goto error;
		}

		if (SET_CHAR_IN_DICT(py_broker, &brokers[idx], host) == -1) {
			goto error;
		}

		if (SET_LONG_IN_DICT(py_broker, &brokers[idx], port) == -1) {
			goto error;
		}

		py_brokers[idx].id = brokers[idx].id;
		py_brokers[idx].py_broker = py_broker;
	}

	return py_brokers;

error:
	for (idx = 0; idx < broker_cnt; idx++) {
		Py_XDECREF(py_brokers[idx].py_broker);
	}

	free(py_brokers);

	return NULL;
}

static PyObject *
metadata_to_dict(const rd_kafka_metadata_t *metadata)
{
    PyObject *py_topics = NULL;
    struct broker_entry *py_brokers = NULL;
    int idx;

    py_brokers = make_broker_objects(metadata->broker_cnt, metadata->brokers);
    if (py_brokers == NULL) {
        return NULL;
    }

    py_topics = topics_to_dict(metadata->topic_cnt, metadata->topics,
			       py_brokers, metadata->broker_cnt);
    for (idx = 0; idx < metadata->broker_cnt; idx++) {
	    Py_DECREF(py_brokers[idx].py_broker);
    }

    free(py_brokers);

    return py_topics;
}


PyObject *
get_metadata(Handle *self, PyObject *args, PyObject *kwargs)
{
    CallState cs;
    PyObject *result = NULL;

    rd_kafka_resp_err_t err;
    const rd_kafka_metadata_t *metadata = NULL;
    rd_kafka_topic_t *only_rkt = NULL;

    const char *topic = NULL;
    double timeout = -1.0f;
    static char *kws[] = {"topic", "timeout", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zd", kws,
                                     &topic, &timeout))
    {
        return NULL;
    }

    if (topic != NULL) {
        if ((only_rkt = rd_kafka_topic_new(self->rk, topic, NULL)) == NULL) {
            return PyErr_Format(PyExc_RuntimeError, "Cannot allocate topic");
        }
    }

    CallState_begin(self, &cs);

    err = rd_kafka_metadata(self->rk, !only_rkt, only_rkt, &metadata,
        timeout >= 0 ? (int)(timeout * 1000.0f) : -1);

    if (!CallState_end(self, &cs)) {
        goto end;
    }

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        cfl_PyErr_Format(
            err, "Failed to get metadata: %s", rd_kafka_err2str(err));

        goto end;
    }

    result = metadata_to_dict(metadata);

end:
    if (metadata != NULL) {
        rd_kafka_metadata_destroy(metadata);
    }

    if (only_rkt != NULL) {
        rd_kafka_topic_destroy(only_rkt);
    }

    return result;
}

const char get_metadata_doc[] = PyDoc_STR(
".. py:function:: list_topics([topic=None], [timeout=-1])\n"
"\n"
" Request Metadata from broker.\n"
" :param bool all_topics: If True => request info about all topics in the cluster."
"Otherwise request info about locally known topics.\n"
" :param str topic: Only request info about this topc (Only used if all_topics=False). \n"
" :param float timeout: Maximum response time before failing. \n"
" :rtype: dict \n"
" :raises: KafkaException \n");
