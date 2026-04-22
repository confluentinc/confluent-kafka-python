/**
 * Copyright 2026 Confluent Inc.
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

/**
 * ⚠️  WARNING: UPDATE TYPE STUBS WHEN MODIFYING INTERFACES ⚠️
 *
 * This file defines the ShareConsumer class and its methods.
 * When changing method signatures, parameters, or defaults, you MUST
 * also update the corresponding type definitions in:
 *   src/confluent_kafka/cimpl.pyi
 *
 * Failure to keep both in sync will result in incorrect type hints.
 */

#include "confluent_kafka.h"

/****************************************************************************
 *
 *
 * ShareConsumer
 *
 *
 ****************************************************************************/

typedef struct {
        Handle base;
        /* base.rk, base.u unused: share uses rkshare, no rebalance */

        rd_kafka_share_t *rkshare;

        /* TODO KIP-932: Remove after interface of librdkafka is updated to
         * return double pointer */
        size_t batch_size;

} ShareConsumerHandle;


static int ShareConsumer_clear(ShareConsumerHandle *self) {
        Handle_clear(&self->base);
        return 0;
}

static void ShareConsumer_dealloc(ShareConsumerHandle *self) {
        PyObject_GC_UnTrack(self);

        if (self->rkshare) {
                CallState cs;
                CallState_begin(&self->base, &cs);
                /* TODO KIP-932: Use rd_kafka_share_destroy_flags() once
                 * available in the librdkafka public API. */
                rd_kafka_share_destroy(self->rkshare);
                self->rkshare = NULL;
                CallState_end(&self->base, &cs);
        }

        Handle_clear(&self->base);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
ShareConsumer_traverse(ShareConsumerHandle *self, visitproc visit, void *arg) {
        return Handle_traverse(&self->base, visit, arg);
}


/**
 * @brief Subscribe to topics.
 */
static PyObject *ShareConsumer_subscribe(ShareConsumerHandle *self,
                                         PyObject *args,
                                         PyObject *kwargs) {
        PyObject *tlist;
        static char *kws[] = {"topics", NULL};
        rd_kafka_topic_partition_list_t *c_topics;
        rd_kafka_resp_err_t err;
        Py_ssize_t i;

        if (!self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kws, &tlist))
                return NULL;

        if (!PyList_Check(tlist)) {
                PyErr_SetString(PyExc_TypeError,
                                "expected list of topic unicode strings");
                return NULL;
        }

        /* Build partition list from topic name strings */
        c_topics = rd_kafka_topic_partition_list_new((int)PyList_Size(tlist));
        for (i = 0; i < PyList_Size(tlist); i++) {
                PyObject *o = PyList_GetItem(tlist, i);
                PyObject *uo, *uo8 = NULL;
                if (!(uo = cfl_PyObject_Unistr(o))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected list of unicode strings");
                        rd_kafka_topic_partition_list_destroy(c_topics);
                        return NULL;
                }
                rd_kafka_topic_partition_list_add(c_topics,
                                                  cfl_PyUnistr_AsUTF8(uo, &uo8),
                                                  RD_KAFKA_PARTITION_UA);
                Py_XDECREF(uo8);
                Py_DECREF(uo);
        }

        err = rd_kafka_share_subscribe(self->rkshare, c_topics);

        rd_kafka_topic_partition_list_destroy(c_topics);

        if (err) {
                cfl_PyErr_Format(err, "Failed to subscribe: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Unsubscribe from current subscription.
 */
static PyObject *ShareConsumer_unsubscribe(ShareConsumerHandle *self,
                                           PyObject *ignore) {
        rd_kafka_resp_err_t err;

        if (!self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        err = rd_kafka_share_unsubscribe(self->rkshare);

        if (err) {
                cfl_PyErr_Format(err, "Failed to unsubscribe: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Get current topic subscription.
 */
static PyObject *ShareConsumer_subscription(ShareConsumerHandle *self,
                                            PyObject *ignore) {
        rd_kafka_topic_partition_list_t *c_topics;
        rd_kafka_resp_err_t err;
        PyObject *topics;
        int i;

        if (!self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        err = rd_kafka_share_subscription(self->rkshare, &c_topics);

        if (err) {
                cfl_PyErr_Format(err, "Failed to get subscription: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        /* Return List[str] of topic names, not List[TopicPartition]. */
        topics = PyList_New(c_topics->cnt);
        for (i = 0; i < c_topics->cnt; i++) {
                PyList_SET_ITEM(topics, i,
                                PyUnicode_FromString(c_topics->elems[i].topic));
        }

        rd_kafka_topic_partition_list_destroy(c_topics);

        return topics;
}


/**
 * @brief Poll for a batch of messages from the share consumer.
 *
 */
static PyObject *ShareConsumer_poll(ShareConsumerHandle *self,
                                    PyObject *args,
                                    PyObject *kwargs) {
        double tmout                    = -1.0f;
        static char *kws[]              = {"timeout", NULL};
        rd_kafka_message_t **rkmessages = NULL;
        size_t rkmessages_size          = 0;
        rd_kafka_error_t *error         = NULL;
        PyObject *msglist;
        CallState cs;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms;
        int chunk_timeout_ms;
        int chunk_count = 0;
        size_t i;

        if (!self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;


        rkmessages = malloc(self->batch_size * sizeof(*rkmessages));
        if (!rkmessages) {
                PyErr_SetString(PyExc_MemoryError,
                                "Failed to allocate message array");
                return NULL;
        }

        total_timeout_ms = cfl_timeout_ms(tmout);

        CallState_begin(&self->base, &cs);

        /* Chunked polling pattern for signal interruptibility */
        while (1) {
                chunk_timeout_ms = calculate_chunk_timeout(
                    total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);
                if (chunk_timeout_ms == 0) {
                        /* Timeout expired */
                        break;
                }

                /* Consume batch with chunk timeout */
                error = rd_kafka_share_consume_batch(
                    self->rkshare, chunk_timeout_ms, rkmessages,
                    &rkmessages_size);

                /* Exit on error */
                if (error) {
                        break;
                }

                /* Exit if messages received */
                if (rkmessages_size > 0) {
                        break;
                }

                chunk_count++;

                /* Check for Ctrl+C before next chunk */
                if (check_signals_between_chunks(&self->base, &cs)) {
                        free(rkmessages);
                        return NULL;
                }
        }

        if (!CallState_end(&self->base, &cs)) {
                for (i = 0; i < rkmessages_size; i++)
                        rd_kafka_message_destroy(rkmessages[i]);
                free(rkmessages);
                if (error)
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        /* Handle error from rd_kafka_share_consume_batch() */
        if (error) {
                cfl_PyErr_from_error_destroy(error);
                free(rkmessages);
                return NULL;
        }

        /* Build Python list from all returned messages. */
        msglist = PyList_New(rkmessages_size);

        for (i = 0; i < rkmessages_size; i++) {
                PyObject *msgobj = Message_new0(&self->base, rkmessages[i]);

#ifdef RD_KAFKA_V_HEADERS
                /** Have to detach headers outside Message_new0 because it
                 * declares the rk message as a const */
                rd_kafka_message_detach_headers(
                    rkmessages[i], &((Message *)msgobj)->c_headers);
#endif
                PyList_SET_ITEM(msglist, i, msgobj);
                rd_kafka_message_destroy(rkmessages[i]);
        }

        free(rkmessages);

        return msglist;
}


/**
 * @brief Close the share consumer.
 */
static PyObject *ShareConsumer_close(ShareConsumerHandle *self,
                                     PyObject *ignore) {
        rd_kafka_resp_err_t err;
        CallState cs;

        if (!self->rkshare)
                Py_RETURN_NONE;

        CallState_begin(&self->base, &cs);
        /* TODO KIP-932: rd_kafka_share_consumer_close() return type will change
         * to rd_kafka_error_t *. Update error handling accordingly. */
        err = rd_kafka_share_consumer_close(self->rkshare);
        rd_kafka_share_destroy(self->rkshare);
        self->rkshare = NULL;
        if (!CallState_end(&self->base, &cs))
                return NULL;

        if (err) {
                cfl_PyErr_Format(err, "Failed to close consumer: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Context manager entry — returns self.
 */
static PyObject *ShareConsumer_enter(ShareConsumerHandle *self,
                                     PyObject *ignore) {
        Py_INCREF(self);
        return (PyObject *)self;
}

/**
 * @brief Context manager exit — calls close().
 */
static PyObject *ShareConsumer_exit(ShareConsumerHandle *self, PyObject *args) {
        PyObject *exc_type, *exc_value, *exc_traceback;

        if (!PyArg_UnpackTuple(args, "__exit__", 3, 3, &exc_type, &exc_value,
                               &exc_traceback))
                return NULL;

        /* Cleanup: call close() */
        if (self->rkshare) {
                PyObject *result = ShareConsumer_close(self, NULL);
                if (!result)
                        return NULL;
                Py_DECREF(result);
        }

        Py_RETURN_NONE;
}


/**
 * @brief ShareConsumer methods.
 */
static PyMethodDef ShareConsumer_methods[] = {
    {"subscribe", (PyCFunction)ShareConsumer_subscribe,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: subscribe(topics)\n"
     "\n"
     "  Set subscription to supplied list of topics\n"
     "\n"
     "  :param list(str) topics: List of topics to subscribe to.\n"
     "  :raises KafkaException: on error\n"
     "  :raises RuntimeError: if called on a closed share consumer\n"
     "\n"},

    {"unsubscribe", (PyCFunction)ShareConsumer_unsubscribe, METH_NOARGS,
     ".. py:function:: unsubscribe()\n"
     "\n"
     "  Unsubscribe from the current topic subscription.\n"
     "\n"
     "  :raises KafkaException: on error\n"
     "  :raises RuntimeError: if called on a closed share consumer\n"
     "\n"},

    {"subscription", (PyCFunction)ShareConsumer_subscription, METH_NOARGS,
     ".. py:function:: subscription()\n"
     "\n"
     "  Get current topic subscription.\n"
     "\n"
     "  :returns: List of subscribed topics\n"
     "  :rtype: list(str)\n"
     "  :raises KafkaException: on error\n"
     "  :raises RuntimeError: if called on a closed share consumer\n"
     "\n"},

    {"poll", (PyCFunction)ShareConsumer_poll, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: poll([timeout=-1])\n"
     "\n"
     "  Poll for a batch of messages from the share consumer.\n"
     "\n"
     "  The application must check each Message object's error() method\n"
     "  to distinguish between proper messages (error() returns None)\n"
     "  and errors.\n"
     "\n"
     "  :param float timeout: Maximum time to block waiting for messages "
     "(seconds).\n"
     "                        Default: -1 (infinite)\n"
     "  :returns: List of Message objects (possibly empty on timeout)\n"
     "  :rtype: list(Message)\n"
     "  :raises KafkaException: on error\n"
     "  :raises RuntimeError: if called on a closed share consumer\n"
     "  :raises KeyboardInterrupt: if Ctrl+C pressed during consumption\n"
     "\n"},

    {"close", (PyCFunction)ShareConsumer_close, METH_NOARGS,
     ".. py:function:: close()\n"
     "\n"
     "  Close the share consumer.\n"
     "\n"
     "  This method should be called to properly clean up the share consumer\n"
     "  and leave the share group.\n"
     "\n"
     "  :raises KafkaException: on error\n"
     "\n"},

    /* TODO KIP-932: Add set_sasl_credentials once librdkafka exposes
     * rd_kafka_sasl_set_credentials() (or the underlying rd_kafka_t *)
     * for rd_kafka_share_t handles. */

    {"__enter__", (PyCFunction)ShareConsumer_enter, METH_NOARGS,
     "Context manager entry."},
    {"__exit__", (PyCFunction)ShareConsumer_exit, METH_VARARGS,
     "Context manager exit. Automatically closes the share consumer."},

    {NULL}};


/**
 * @brief Initialize ShareConsumer.
 */
static int
ShareConsumer_init(PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        ShareConsumerHandle *self = (ShareConsumerHandle *)selfobj;
        char errstr[512];
        rd_kafka_conf_t *conf;

        if (self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                "ShareConsumer already initialized");
                return -1;
        }

        self->base.type = RD_KAFKA_CONSUMER;

        if (!(conf = common_conf_setup(RD_KAFKA_CONSUMER, &self->base, args,
                                       kwargs)))
                return -1; /* Exception raised by common_conf_setup() */

        /* TODO KIP-932: Remove after interface of librdkafka is updated to
         * return double pointer */
        self->batch_size = 10005;

        self->rkshare =
            rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!self->rkshare) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create share consumer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* TODO KIP-932: call rd_kafka_set_log_queue() once librdkafka adds a
         * rd_kafka_share_set_log_queue() wrapper — needs rd_kafka_t *, which
         * is opaque inside rd_kafka_share_t in the public API. */

        /* TODO KIP-932: call rd_kafka_sasl_background_callbacks_enable() for
         * OAuth once librdkafka adds a share-level wrapper for the same reason.
         */


        return 0;
}


/**
 * @brief ShareConsumer __new__ method.
 */
static PyObject *
ShareConsumer_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}


/**
 * @brief ShareConsumer type definition.
 */
PyTypeObject ShareConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "cimpl.ShareConsumer", /*tp_name*/
    sizeof(ShareConsumerHandle),                          /*tp_basicsize*/
    0,                                                    /*tp_itemsize*/
    (destructor)ShareConsumer_dealloc,                    /*tp_dealloc*/
    0,                                                    /*tp_print*/
    0,                                                    /*tp_getattr*/
    0,                                                    /*tp_setattr*/
    0,                                                    /*tp_compare*/
    0,                                                    /*tp_repr*/
    0,                                                    /*tp_as_number*/
    0,                                                    /*tp_as_sequence*/
    0,                                                    /*tp_as_mapping*/
    0,                                                    /*tp_hash */
    0,                                                    /*tp_call*/
    0,                                                    /*tp_str*/
    0,                                                    /*tp_getattro*/
    0,                                                    /*tp_setattro*/
    0,                                                    /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "A high-level Apache Kafka share consumer\n"
    "\n"
    ".. py:function:: ShareConsumer(config)\n"
    "\n"
    "Create a new ShareConsumer instance using the provided configuration "
    "*dict*.\n"
    "Share consumers enable queue-like consumption where each partition can be "
    "assigned to multiple consumers. Messages are delivered to only one "
    "consumer.\n"
    "\n"
    ":param dict config: Configuration properties. At a minimum, "
    "``group.id`` **must** be set and ``bootstrap.servers`` **should** be "
    "set.\n"
    "\n",                                 /*tp_doc*/
    (traverseproc)ShareConsumer_traverse, /* tp_traverse */
    (inquiry)ShareConsumer_clear,         /* tp_clear */
    0,                                    /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    ShareConsumer_methods,                /* tp_methods */
    0,                                    /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    ShareConsumer_init,                   /* tp_init */
    0,                                    /* tp_alloc */
    ShareConsumer_new                     /* tp_new */
};
