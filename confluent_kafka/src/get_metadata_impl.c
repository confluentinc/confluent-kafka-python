#include "confluent_kafka.h"

#if RD_KAFKA_VERSION >= 0x00080400

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

    py_val = PyUnicode_FromString(val);

    if (py_val == NULL) {
        return -1;
    }

    err = PyDict_SetItemString(target, key, py_val);
    Py_DECREF(py_val);
    return err;
}


static int
set_intarray_in_dict(PyObject *target, const char *key, int cnt, int32_t *int_array)
{
    int err;
    int idx;
    PyObject *py_val = NULL;
    PyObject *tmp_pyo = NULL;

    if ((py_val = PyTuple_New(cnt)) == NULL) {
        return -1;
    }

    for (idx = 0; idx < cnt; ++idx) {
        if ((tmp_pyo = PyLong_FromLong(int_array[idx])) == NULL) {
            Py_DECREF(py_val);
            return -1;
        }

        PyTuple_SET_ITEM(py_val, idx, tmp_pyo);
    }

    err = PyDict_SetItemString(target, key, py_val);
    Py_DECREF(py_val);
    return err;
}


#define SET_LONG_IN_DICT(target, ptr, member) \
    set_long_in_dict(target, #member, (ptr)->member)


#define SET_CHAR_IN_DICT(target, ptr, member) \
    set_char_in_dict(target, #member, (ptr)->member)


static PyObject *
partitions_to_dict(
    int partition_cnt,
    const rd_kafka_metadata_partition_t *partitions)
{
    int err;
    int idx;

    PyObject *py_partitions = NULL;
    PyObject *py_part = NULL;

    if ((py_partitions = PyDict_New()) == NULL) {
        goto error;
    }

    for (idx = 0; idx < partition_cnt; ++idx) {
        if ((py_part = PyDict_New()) == NULL) {
            goto error;
        }

        if (SET_LONG_IN_DICT(py_part, &partitions[idx], id) == -1) {
            goto error;
        }

        if (SET_LONG_IN_DICT(py_part, &partitions[idx], leader) == -1) {
            goto error;
        }

        if (SET_LONG_IN_DICT(py_part, &partitions[idx], replica_cnt) == -1) {
            goto error;
        }

        if (SET_LONG_IN_DICT(py_part, &partitions[idx], isr_cnt) == -1) {
            goto error;
        }

        err = set_intarray_in_dict(
            py_part,
            "replicas",
            partitions[idx].replica_cnt,
            partitions[idx].replicas);

        if (err == -1) {
            goto error;
        }

        err = set_intarray_in_dict(
            py_part,
            "isrs",
            partitions[idx].isr_cnt,
            partitions[idx].isrs);

        if (err == -1) {
            goto error;
        }

        err = PyDict_SetItem(
            py_partitions,
            PyDict_GetItemString(py_part, "id"),
            py_part);

        if (err == -1) {
            goto error;
        }

        Py_DECREF(py_part);
    }

    return py_partitions;

error:
    Py_XDECREF(py_partitions);
    Py_XDECREF(py_part);
    return NULL;
}


static PyObject *
topics_to_dict(int topic_cnt, const rd_kafka_metadata_topic_t *topics)
{
    int err;
    int idx;
    PyObject *py_topcis = NULL;
    PyObject *py_topic = NULL;
    PyObject *tmp_pyo = NULL;

    if ((py_topcis = PyDict_New()) == NULL) {
        goto error;
    }

    for (idx=0; idx < topic_cnt; ++idx) {
        if ((py_topic = PyDict_New()) == NULL) {
            goto error;
        }

        if (SET_LONG_IN_DICT(py_topic, &topics[idx], partition_cnt) == -1) {
            goto error;
        }

        if (SET_CHAR_IN_DICT(py_topic, &topics[idx], topic) == -1) {
            goto error;
        }

        tmp_pyo = partitions_to_dict(
            topics[idx].partition_cnt,
            topics[idx].partitions);

        if (tmp_pyo == NULL) {
            goto error;
        }

        if (PyDict_SetItemString(py_topic, "partitions", tmp_pyo) == -1) {
            goto error;
        }

        Py_DECREF(tmp_pyo);

        err = PyDict_SetItem(
            py_topcis,
            PyDict_GetItemString(py_topic, "topic"),
            py_topic);

        if (err == -1) {
            goto error;
        }

        Py_DECREF(py_topic);

    }

    return py_topcis;

error:
    Py_XDECREF(py_topcis);
    Py_XDECREF(py_topic);
    Py_XDECREF(tmp_pyo);
    return NULL;
}


static PyObject *
brokers_to_dict(int broker_cnt, const rd_kafka_metadata_broker_t *brokers)
{
    int err;
    int idx;
    PyObject *py_brokers = NULL;
    PyObject *py_broker = NULL;

    if ((py_brokers = PyDict_New()) == NULL) {
        goto error;
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

        err = PyDict_SetItem(
            py_brokers,
            PyDict_GetItemString(py_broker, "id"),
            py_broker);

        if (err == -1) {
            goto error;
        }

        Py_DECREF(py_broker);
    }

    return py_brokers;

error:
    Py_XDECREF(py_brokers);
    Py_XDECREF(py_broker);
    return NULL;
}


static PyObject *
metadata_to_dict(const rd_kafka_metadata_t *metadata)
{
    PyObject *result = NULL;
    PyObject *tmp_pyo = NULL;

    if ((result = PyDict_New()) == NULL) {
        goto error;
    }

    if (SET_LONG_IN_DICT(result, metadata, broker_cnt) == -1) {
        goto error;
    }

    if (SET_LONG_IN_DICT(result, metadata, topic_cnt) == -1) {
        goto error;
    }

    if (SET_LONG_IN_DICT(result, metadata, orig_broker_id) == -1) {
        goto error;
    }

    if (SET_CHAR_IN_DICT(result, metadata, orig_broker_name) == -1) {
        goto error;
    }

    tmp_pyo = brokers_to_dict(metadata->broker_cnt, metadata->brokers);
    if (tmp_pyo == NULL) {
        goto error;
    }

    if (PyDict_SetItemString(result, "brokers", tmp_pyo) == -1) {
        goto error;
    }

    Py_DECREF(tmp_pyo);

    tmp_pyo = topics_to_dict(metadata->topic_cnt, metadata->topics);
    if (tmp_pyo == NULL) {
        goto error;
    }

    if (PyDict_SetItemString(result, "topics", tmp_pyo) == -1) {
        goto error;
    }

    Py_DECREF(tmp_pyo);

    return result;

error:
    Py_XDECREF(result);
    Py_XDECREF(tmp_pyo);
    return NULL;
}


PyObject *
Kafka_generic_get_metadata(Handle *self, PyObject *args, PyObject *kwargs)
{
    CallState cs;
    PyObject *result;

    rd_kafka_resp_err_t err;
    const rd_kafka_metadata_t *metadata = NULL;
    rd_kafka_topic_t *only_rkt = NULL;

    int all_topics = 1;
    const char *topic = NULL;
    double timeout = -1.0f;
    static char *kws[] = {"all_topics", "topic", "timeout", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|izd", kws,
                                     &all_topics, &topic, &timeout))
    {
        return NULL;
    }

    if (topic != NULL) {
        if ((only_rkt = rd_kafka_topic_new(self->rk, topic, NULL)) == NULL) {
            return PyErr_Format(PyExc_RuntimeError, "Cannot allocate topic");
        }
    }

    CallState_begin(self, &cs);

    err = rd_kafka_metadata(
        self->rk,
        all_topics,
        only_rkt,
        &metadata,
        timeout >= 0 ? (int)(timeout * 1000.0f) : -1);

    if (!CallState_end(self, &cs)) {
        goto end;
    }

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        cfl_PyErr_Format(
            err, "Failed to get metadata: %s", rd_kafka_err2str(err));

        result = NULL;
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

#else

/* Raise an NotImplemneted Error if librdkafa is too old. */
PyObject *
Kafka_generic_get_metadata(Handle *self, PyObject *args, PyObject *kwargs)
{
    return PyErr_Format(PyExc_NotImplementedError, "librdkafka is too old.");
}

#endif
