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

#define ERR_MSG_SHARE_CONSUMER_CLOSED "Share consumer closed"

/****************************************************************************
 *
 *
 * ShareConsumer
 *
 *
 ****************************************************************************/

typedef struct {
        Handle base;  /* first member — (Handle *)self cast is always safe */
                      /* base.rk, base.u unused: share uses rkshare, no rebalance */

        rd_kafka_share_t *rkshare;

        /* TODO: Remove after interface of librdkafka is updated to return double pointer */
        size_t max_poll_records;

} ShareConsumerHandle;


static int ShareConsumer_clear(ShareConsumerHandle *self) {
        Handle_clear((Handle *)self);
        return 0;
}

static void ShareConsumer_dealloc(ShareConsumerHandle *self) {
        PyObject_GC_UnTrack(self);

        if (self->rkshare) {
                CallState cs;
                CallState_begin((Handle *)self, &cs);
                rd_kafka_share_destroy(self->rkshare);
                self->rkshare = NULL;
                CallState_end((Handle *)self, &cs);
        }

        Handle_clear((Handle *)self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int ShareConsumer_traverse(ShareConsumerHandle *self,
                                  visitproc visit,
                                  void *arg) {
        return Handle_traverse((Handle *)self, visit, arg);
}


/**
 * @brief Subscribe to topics.
 */
static PyObject *
ShareConsumer_subscribe(ShareConsumerHandle *self,
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
                PyObject *o   = PyList_GetItem(tlist, i);
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
static PyObject *ShareConsumer_unsubscribe(ShareConsumerHandle *self) {
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
static PyObject *ShareConsumer_subscription(ShareConsumerHandle *self) {
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
 * @brief Consume a batch of messages from the share consumer.
 *
 */
static PyObject *
ShareConsumer_consume_batch(ShareConsumerHandle *self,
                            PyObject *args,
                            PyObject *kwargs) {
        double tmout              = -1.0f;
        static char *kws[]        = {"timeout", NULL};
        rd_kafka_message_t **rkmessages = NULL;
        size_t rkmessages_size = 0;
        rd_kafka_error_t *error = NULL;
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


        rkmessages = malloc(self->max_poll_records * sizeof(*rkmessages));
        if (!rkmessages) {
                PyErr_SetString(PyExc_MemoryError, "Failed to allocate message array");
                return NULL;
        }

        total_timeout_ms = cfl_timeout_ms(tmout);

        CallState_begin((Handle *)self, &cs);

        /* Chunked polling pattern for signal interruptibility */
        if (total_timeout_ms >= 0 && total_timeout_ms < CHUNK_TIMEOUT_MS) {
                /* Short timeout: single call */
                error = rd_kafka_share_consume_batch(self->rkshare,
                                                     total_timeout_ms,
                                                     rkmessages,
                                                     &rkmessages_size);
        } else {
                /* Long timeout: chunked with signal checking */
                while (1) {
                        /* Calculate timeout for this chunk */
                        if (total_timeout_ms < 0) {
                                /* Infinite timeout */
                                chunk_timeout_ms = CHUNK_TIMEOUT_MS;
                        } else {
                                int remaining = total_timeout_ms -
                                                (chunk_count * CHUNK_TIMEOUT_MS);
                                if (remaining <= 0) {
                                        /* Timeout expired */
                                        break;
                                }
                                chunk_timeout_ms = remaining < CHUNK_TIMEOUT_MS
                                                       ? remaining
                                                       : CHUNK_TIMEOUT_MS;
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

                        /* No messages yet — check for Ctrl+C before next chunk. */
                        chunk_count++;
                        if (check_signals_between_chunks((Handle *)self, &cs)) {
                                free(rkmessages);
                                return NULL;
                        }
                }
        }

        if (!CallState_end((Handle *)self, &cs)) {
                for (i = 0; i < rkmessages_size; i++)
                        rd_kafka_message_destroy(rkmessages[i]);
                free(rkmessages);
                if (error)
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        /* Handle error from rd_kafka_share_consume_batch() */
        if (error) {
                const char *error_str = rd_kafka_error_string(error);
                int is_fatal          = rd_kafka_error_is_fatal(error);
                int is_retriable      = rd_kafka_error_is_retriable(error);

                if (is_fatal) {
                        PyErr_Format(PyExc_RuntimeError, "Fatal error: %s",
                                     error_str);
                } else {
                        PyErr_Format(KafkaException, "Error: %s (retriable: %s)",
                                     error_str, is_retriable ? "yes" : "no");
                }

                rd_kafka_error_destroy(error);
                free(rkmessages);
                return NULL;
        }

        /* Build Python list from all returned messages. */
        msglist = PyList_New(rkmessages_size);

        for (i = 0; i < rkmessages_size; i++) {
                PyObject *msgobj =
                    Message_new0((Handle *)self, rkmessages[i]);

#ifdef RD_KAFKA_V_HEADERS
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
static PyObject *ShareConsumer_close(ShareConsumerHandle *self) {
        rd_kafka_resp_err_t err;
        CallState cs;

        if (!self->rkshare)
                Py_RETURN_NONE;

        CallState_begin((Handle *)self, &cs);
        err = rd_kafka_share_consumer_close(self->rkshare);
        rd_kafka_share_destroy(self->rkshare);
        self->rkshare = NULL;
        if (!CallState_end((Handle *)self, &cs))
                return NULL;

        if (err) {
                cfl_PyErr_Format(err, "Failed to close consumer: %s",
                                 rd_kafka_err2str(err));
                return NULL;
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

    {"consume_batch", (PyCFunction)ShareConsumer_consume_batch,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: consume_batch([timeout=-1])\n"
     "\n"
     "  Consume a batch of messages from the share consumer.\n"
     "\n"
     "  This is the ONLY consumption method for ShareConsumer.\n"
     "  Share consumers do NOT have a poll() method - they are batch-only.\n"
     "\n"
     "  The application must check each Message object's error() method\n"
     "  to distinguish between proper messages (error() returns None)\n"
     "  and errors.\n"
     "\n"
     "  Batch size is controlled by the ``max.poll.records`` configuration\n"
     "  property, not a runtime argument. Records are locked by the broker\n"
     "  per fetch cycle and must not be discarded mid-batch.\n"
     "\n"
     "  :param float timeout: Maximum time to block waiting for messages "
     "(seconds).\n"
     "                        Default: -1 (infinite)\n"
     "  :returns: List of Message objects (possibly empty on timeout)\n"
     "  :rtype: list(Message)\n"
     "  :raises RuntimeError: if called on a closed share consumer or on "
     "fatal error\n"
     "  :raises KafkaException: on non-fatal errors\n"
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

    {NULL}};


/**
 * @brief Initialize ShareConsumer.
 */
static int ShareConsumer_init(PyObject *selfobj,
                              PyObject *args,
                              PyObject *kwargs) {
        ShareConsumerHandle *self = (ShareConsumerHandle *)selfobj;
        char errstr[512];
        rd_kafka_conf_t *conf;

        if (self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                "ShareConsumer already initialized");
                return -1;
        }

        self->base.type = RD_KAFKA_CONSUMER;

        if (!(conf = common_conf_setup(RD_KAFKA_CONSUMER, (Handle *)self, args,
                                       kwargs)))
                return -1; /* Exception raised by common_conf_setup() */

        /* TODO: Remove after interface of librdkafka is updated to return double pointer */
        self->max_poll_records = 10005;

        self->rkshare = rd_kafka_share_consumer_new(conf, errstr,
                                                    sizeof(errstr));
        if (!self->rkshare) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create share consumer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* TODO: rd_kafka_share_poll_set_consumer() does not exist in KIP-932 API.
         * Share consumers may have a different event forwarding model.
         * Call this once librdkafka adds a share-level wrapper for it. */

        /* TODO: call rd_kafka_set_log_queue() once librdkafka adds a
         * rd_kafka_share_set_log_queue() wrapper — needs rd_kafka_t *, which
         * is opaque inside rd_kafka_share_t in the public API. */

        /* TODO: call rd_kafka_sasl_background_callbacks_enable() for OAuth once
         * librdkafka adds a share-level wrapper for the same reason. */


        return 0;
}


/**
 * @brief ShareConsumer __new__ method.
 */
static PyObject *ShareConsumer_new(PyTypeObject *type,
                                   PyObject *args,
                                   PyObject *kwargs) {
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
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "A high-level Apache Kafka share consumer (KIP-932)\n"
    "\n"
    ".. py:function:: ShareConsumer(config)\n"
    "\n"
    "Create a new ShareConsumer instance using the provided configuration "
    "*dict*.\n"
    "Share consumers enable queue-like consumption where each partition can be "
    "\n"
    "assigned to multiple consumers. Messages are delivered to only one "
    "consumer.\n"
    "\n"
    ".. note::\n"
    "   ShareConsumer only supports batch consumption via consume_batch().\n"
    "   There is NO poll() method for single messages.\n"
    "\n"
    ":param dict config: Configuration properties. At a minimum, "
    "``group.id`` **must** be set and ``bootstrap.servers`` **should** be "
    "set.\n"
    "\n", /*tp_doc*/
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
