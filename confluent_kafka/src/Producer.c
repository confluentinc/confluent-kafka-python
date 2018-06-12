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


/**
 * @brief KNOWN ISSUES
 *
 *  - Partitioners will cause a dead-lock with librdkafka, because:
 *     GIL + topic lock in topic_new  is different lock order than
 *     topic lock in msg_partitioner + GIL.
 *     This needs to be sorted out in librdkafka, preferably making the
 *     partitioner run without any locks taken.
 *     Until this is fixed the partitioner is ignored and librdkafka's
 *     default will be used.
 *
 */



/****************************************************************************
 *
 *
 * Producer
 *
 *
 *
 *
 ****************************************************************************/

/**
 * Per-message state.
 */
struct Producer_msgstate {
	Handle   *self;
	PyObject *dr_cb;
};


/**
 * Create a new per-message state.
 * Returns NULL if neither dr_cb or partitioner_cb is set.
 */
static __inline struct Producer_msgstate *
Producer_msgstate_new (Handle *self,
		       PyObject *dr_cb) {
	struct Producer_msgstate *msgstate;

	msgstate = calloc(1, sizeof(*msgstate));
	msgstate->self = self;

	if (dr_cb) {
		msgstate->dr_cb = dr_cb;
		Py_INCREF(dr_cb);
	}
	return msgstate;
}

static __inline void
Producer_msgstate_destroy (struct Producer_msgstate *msgstate) {
	if (msgstate->dr_cb)
		Py_DECREF(msgstate->dr_cb);
	free(msgstate);
}


static void Producer_clear0 (Handle *self) {
        if (self->u.Producer.default_dr_cb) {
                Py_DECREF(self->u.Producer.default_dr_cb);
                self->u.Producer.default_dr_cb = NULL;
        }
}

static int Producer_clear (Handle *self) {
        Producer_clear0(self);
        Handle_clear(self);
        return 0;
}

static void Producer_dealloc (Handle *self) {
	PyObject_GC_UnTrack(self);

        Producer_clear0(self);

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Handle_clear(self);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Producer_traverse (Handle *self,
			      visitproc visit, void *arg) {
	if (self->u.Producer.default_dr_cb)
		Py_VISIT(self->u.Producer.default_dr_cb);

	Handle_traverse(self, visit, arg);

	return 0;
}


static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkm,
			   void *opaque) {
	struct Producer_msgstate *msgstate = rkm->_private;
	Handle *self = opaque;
	CallState *cs;
	PyObject *args;
	PyObject *result;
	PyObject *msgobj;

	if (!msgstate)
		return;

	cs = CallState_get(self);

	if (!msgstate->dr_cb) {
		/* No callback defined */
		goto done;
	}

        /* Skip callback if delivery.report.only.error=true */
        if (self->u.Producer.dr_only_error && !rkm->err)
                goto done;

	msgobj = Message_new0(self, rkm);
	
	args = Py_BuildValue("(OO)",
			     Message_error((Message *)msgobj, NULL),
			     msgobj);

	Py_DECREF(msgobj);

	if (!args) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
				 "Unable to build callback args");
		CallState_crash(cs);
		goto done;
	}

	result = PyObject_CallObject(msgstate->dr_cb, args);
	Py_DECREF(args);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(rk);
	}

 done:
	Producer_msgstate_destroy(msgstate);
	CallState_resume(cs);
}


#if HAVE_PRODUCEV
static rd_kafka_resp_err_t
Producer_producev (Handle *self,
                   const char *topic, int32_t partition,
                   const void *value, size_t value_len,
                   const void *key, size_t key_len,
                   void *opaque, int64_t timestamp
#ifdef RD_KAFKA_V_HEADERS
                   ,rd_kafka_headers_t *headers
#endif
                   ) {

        return rd_kafka_producev(self->rk,
                                 RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                 RD_KAFKA_V_TOPIC(topic),
                                 RD_KAFKA_V_PARTITION(partition),
                                 RD_KAFKA_V_KEY(key, (size_t)key_len),
                                 RD_KAFKA_V_VALUE((void *)value,
                                                  (size_t)value_len),
                                 RD_KAFKA_V_TIMESTAMP(timestamp),
#ifdef RD_KAFKA_V_HEADERS
                                 RD_KAFKA_V_HEADERS(headers),
#endif
                                 RD_KAFKA_V_OPAQUE(opaque),
                                 RD_KAFKA_V_END);
}
#else

static rd_kafka_resp_err_t
Producer_produce0 (Handle *self,
                   const char *topic, int32_t partition,
                   const void *value, size_t value_len,
                   const void *key, size_t key_len,
                   void *opaque) {
        rd_kafka_topic_t *rkt;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

        if (!(rkt = rd_kafka_topic_new(self->rk, topic, NULL)))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

	if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			     (void *)value, value_len,
			     (void *)key, key_len, opaque) == -1)
                err = rd_kafka_last_error();

        rd_kafka_topic_destroy(rkt);

        return err;
}
#endif


static PyObject *Producer_produce (Handle *self, PyObject *args,
				       PyObject *kwargs) {
	const char *topic, *value = NULL, *key = NULL;
	int value_len = 0, key_len = 0;
	int partition = RD_KAFKA_PARTITION_UA;
	PyObject *headers = NULL, *dr_cb = NULL, *dr_cb2 = NULL;
        long long timestamp = 0;
        rd_kafka_resp_err_t err;
	struct Producer_msgstate *msgstate;
#ifdef RD_KAFKA_V_HEADERS
    rd_kafka_headers_t *rd_headers = NULL;
#endif

	static char *kws[] = { "topic",
			       "value",
			       "key",
			       "partition",
			       "callback",
			       "on_delivery", /* Alias */
                   "timestamp",
                   "headers",
			       NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs,
					 "s|z#z#iOOLO"
                                         , kws,
					 &topic, &value, &value_len,
					 &key, &key_len, &partition,
					 &dr_cb, &dr_cb2,
                     &timestamp, &headers))
		return NULL;

#if !HAVE_PRODUCEV
        if (timestamp) {
                PyErr_Format(PyExc_NotImplementedError,
                             "Producer timestamps require "
                             "confluent-kafka-python built for librdkafka "
                             "version >=v0.9.4 (librdkafka runtime 0x%x, "
                             "buildtime 0x%x)",
                             rd_kafka_version(), RD_KAFKA_VERSION);
                return NULL;
        }
#endif

#ifndef RD_KAFKA_V_HEADERS
    if (headers) {
            PyErr_Format(PyExc_NotImplementedError,
                         "Producer message headers requires "
                         "confluent-kafka-python built for librdkafka "
                         "version >=v0.11.4 (librdkafka runtime 0x%x, "
                         "buildtime 0x%x)",
                         rd_kafka_version(), RD_KAFKA_VERSION);
            return NULL;
    }
#else
    if (headers) {
        if(!(rd_headers = py_headers_to_c(headers)))
            return NULL;
    }
#endif


	if (dr_cb2 && !dr_cb) /* Alias */
		dr_cb = dr_cb2;

	if (!dr_cb || dr_cb == Py_None)
		dr_cb = self->u.Producer.default_dr_cb;

	/* Create msgstate if necessary, may return NULL if no callbacks
	 * are wanted. */
	msgstate = Producer_msgstate_new(self, dr_cb);

        /* Produce message */
#if HAVE_PRODUCEV
        err = Producer_producev(self, topic, partition,
                                value, value_len,
                                key, key_len,
                                msgstate, timestamp
#ifdef RD_KAFKA_V_HEADERS
                                ,rd_headers
#endif
                                );
#else
        err = Producer_produce0(self, topic, partition,
                                value, value_len,
                                key, key_len,
                                msgstate);
#endif

        if (err) {
		if (msgstate)
			Producer_msgstate_destroy(msgstate);

		if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
			PyErr_Format(PyExc_BufferError,
				     "%s", rd_kafka_err2str(err));
                else
			cfl_PyErr_Format(err,
					 "Unable to produce message: %s",
					 rd_kafka_err2str(err));

		return NULL;
	}

	Py_RETURN_NONE;
}


/**
 * @brief Call rd_kafka_poll() and keep track of crashing callbacks.
 * @returns -1 if callback crashed (or poll() failed), else the number
 * of events served.
 */
static int Producer_poll0 (Handle *self, int tmout) {
	int r;
	CallState cs;

	CallState_begin(self, &cs);

	r = rd_kafka_poll(self->rk, tmout);

	if (!CallState_end(self, &cs)) {
		return -1;
	}

	return r;
}


static PyObject *Producer_poll (Handle *self, PyObject *args,
				    PyObject *kwargs) {
	double tmout;
	int r;
	static char *kws[] = { "timeout", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "d", kws, &tmout))
		return NULL;

	r = Producer_poll0(self, (int)(tmout * 1000));
	if (r == -1)
		return NULL;

	return cfl_PyInt_FromInt(r);
}


static PyObject *Producer_flush (Handle *self, PyObject *args,
                                 PyObject *kwargs) {
        double tmout = -1;
        int qlen;
        static char *kws[] = { "timeout", NULL };
#if RD_KAFKA_VERSION >= 0x00090300
        CallState cs;
#endif

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

#if RD_KAFKA_VERSION >= 0x00090300
        CallState_begin(self, &cs);
        rd_kafka_flush(self->rk, tmout < 0 ? -1 : (int)(tmout * 1000));
        if (!CallState_end(self, &cs))
                return NULL;
        qlen = rd_kafka_outq_len(self->rk);
#else
        while ((qlen = rd_kafka_outq_len(self->rk)) > 0) {
                if (Producer_poll0(self, 500) == -1)
                        return NULL;
        }
#endif
        return cfl_PyInt_FromInt(qlen);
}

static PyMethodDef Producer_methods[] = {
	{ "produce", (PyCFunction)Producer_produce,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])\n"
	  "\n"
	  "  Produce message to topic.\n"
	  "  This is an asynchronous operation, an application may use the "
	  "``callback`` (alias ``on_delivery``) argument to pass a function "
	  "(or lambda) that will be called from :py:func:`poll()` when the "
	  "message has been successfully delivered or permanently fails delivery.\n"
      "\n"
      "  Currently message headers are not supported on the message returned to the "
      "callback. The ``msg.headers()`` will return None even if the original message "
      "had headers set.\n"
	  "\n"
	  "  :param str topic: Topic to produce message to\n"
	  "  :param str|bytes value: Message payload\n"
	  "  :param str|bytes key: Message key\n"
	  "  :param int partition: Partition to produce to, else uses the "
	  "configured built-in partitioner.\n"
	  "  :param func on_delivery(err,msg): Delivery report callback to call "
	  "(from :py:func:`poll()` or :py:func:`flush()`) on successful or "
	  "failed delivery\n"
          "  :param int timestamp: Message timestamp (CreateTime) in milliseconds since epoch UTC (requires librdkafka >= v0.9.4, api.version.request=true, and broker >= 0.10.0.0). Default value is current time.\n"
	  "\n"
          "  :param headers dict|list: Message headers to set on the message. The header key must be a string while the value must be binary, unicode or None. Accepts a list of (key,value) or a dict. (Requires librdkafka >= v0.11.4 and broker version >= 0.11.0.0)\n"
	  "  :rtype: None\n"
	  "  :raises BufferError: if the internal producer message queue is "
	  "full (``queue.buffering.max.messages`` exceeded)\n"
	  "  :raises KafkaException: for other errors, see exception code\n"
          "  :raises NotImplementedError: if timestamp is specified without underlying library support.\n"
	  "\n"
	},

	{ "poll", (PyCFunction)Producer_poll, METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: poll([timeout])\n"
	  "\n"
	  "  Polls the producer for events and calls the corresponding "
	  "callbacks (if registered).\n"
	  "\n"
	  "  Callbacks:\n"
	  "\n"
	  "  - ``on_delivery`` callbacks from :py:func:`produce()`\n"
	  "  - ...\n"
	  "\n"
	  "  :param float timeout: Maximum time to block waiting for events. (Seconds)\n"
	  "  :returns: Number of events processed (callbacks served)\n"
	  "  :rtype: int\n"
	  "\n"
	},

	{ "flush", (PyCFunction)Producer_flush, METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: flush([timeout])\n"
          "\n"
	  "   Wait for all messages in the Producer queue to be delivered.\n"
	  "   This is a convenience method that calls :py:func:`poll()` until "
	  ":py:func:`len()` is zero or the optional timeout elapses.\n"
	  "\n"
          "  :param: float timeout: Maximum time to block (requires librdkafka >= v0.9.4). (Seconds)\n"
          "  :returns: Number of messages still in queue.\n"
          "\n"
	  ".. note:: See :py:func:`poll()` for a description on what "
	  "callbacks may be triggered.\n"
	  "\n"
	},
        { "list_topics", (PyCFunction)list_topics, METH_VARARGS|METH_KEYWORDS,
          list_topics_doc
        },

	{ NULL }
};


static Py_ssize_t Producer__len__ (Handle *self) {
	return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Producer_seq_methods = {
	(lenfunc)Producer__len__ /* sq_length */
};


static int Producer_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Producer already __init__:ialized");
                return -1;
        }

        self->type = RD_KAFKA_PRODUCER;

        if (!(conf = common_conf_setup(RD_KAFKA_PRODUCER, self,
                                       args, kwargs)))
                return -1;

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        self->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create producer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Forward log messages to poll queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        return 0;
}


static PyObject *Producer_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}



PyTypeObject ProducerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Producer",        /*tp_name*/
	sizeof(Handle),      /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)Producer_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	0,                         /*tp_as_number*/
	&Producer_seq_methods,  /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "Asynchronous Kafka Producer\n"
        "\n"
        ".. py:function:: Producer(config)\n"
        "\n"
        "  :param dict config: Configuration properties. At a minimum ``bootstrap.servers`` **should** be set\n"
        "\n"
        "  Create new Producer instance using provided configuration dict.\n"
        "\n"
        "\n"
        ".. py:function:: len()\n"
        "\n"
        "  :returns: Number of messages and Kafka protocol requests waiting to be delivered to broker.\n"
        "  :rtype: int\n"
        "\n", /*tp_doc*/
	(traverseproc)Producer_traverse, /* tp_traverse */
	(inquiry)Producer_clear, /* tp_clear */
	0,		           /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	Producer_methods,      /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
        Producer_init,             /* tp_init */
	0,                         /* tp_alloc */
	Producer_new           /* tp_new */
};




