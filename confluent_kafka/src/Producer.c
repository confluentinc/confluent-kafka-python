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
	Producer *self;
	PyObject *dr_cb;
	PyObject *partitioner_cb;
};


/**
 * Create a new per-message state.
 * Returns NULL if neither dr_cb or partitioner_cb is set.
 */
static __inline struct Producer_msgstate *
Producer_msgstate_new (Producer *self,
			   PyObject *dr_cb, PyObject *partitioner_cb) {
	struct Producer_msgstate *msgstate;

	if (!dr_cb && !partitioner_cb)
		return NULL;

	msgstate = calloc(1, sizeof(*msgstate));
	msgstate->self = self;

	if (dr_cb) {
		msgstate->dr_cb = dr_cb;
		Py_INCREF(dr_cb);
	}
	if (partitioner_cb) {
		msgstate->partitioner_cb = partitioner_cb;
		Py_INCREF(partitioner_cb);
	}
	return msgstate;
}

static __inline void
Producer_msgstate_destroy (struct Producer_msgstate *msgstate) {
	if (msgstate->dr_cb)
		Py_DECREF(msgstate->dr_cb);
	if (msgstate->partitioner_cb)
		Py_DECREF(msgstate->partitioner_cb);
	free(msgstate);
}


static int Producer_clear (Producer *self) {
	if (self->default_dr_cb) {
		Py_DECREF(self->default_dr_cb);
		self->default_dr_cb = NULL;
	}
	if (self->partitioner_cb) {
		Py_DECREF(self->partitioner_cb);
		self->partitioner_cb = NULL;
	}
	return 0;
}

static void Producer_dealloc (Producer *self) {
	PyObject_GC_UnTrack(self);

	Producer_clear(self);

	if (self->rk)
		rd_kafka_destroy(self->rk);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Producer_traverse (Producer *self,
				  visitproc visit, void *arg) {
	if (self->default_dr_cb)
		Py_VISIT(self->default_dr_cb);
	if (self->partitioner_cb)
		Py_VISIT(self->partitioner_cb);
	return 0;
}


static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkm,
			   void *opaque) {
	struct Producer_msgstate *msgstate = rkm->_private;
	Producer *self = opaque;
	PyObject *args;
	PyObject *result;
	PyObject *msgobj;

	if (!msgstate)
		return;

	PyEval_RestoreThread(self->thread_state);

	if (!msgstate->dr_cb) {
		/* No callback defined */
		goto done;
	}

	msgobj = Message_new0(rkm);
	
	args = Py_BuildValue("(OO)",
			     Message_error((Message *)msgobj, NULL),
			     msgobj);

	Py_DECREF(msgobj);

	if (!args) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
				 "Unable to build callback args");
		self->callback_crashed++;
		goto done;
	}

	result = PyObject_CallObject(msgstate->dr_cb, args);
	Py_DECREF(args);

	if (result)
		Py_DECREF(result);
	else {
		self->callback_crashed++;
		rd_kafka_yield(rk);
	}

 done:
	Producer_msgstate_destroy(msgstate);
	self->thread_state = PyEval_SaveThread();
}


/**
 * FIXME: The partitioner is currently broken due to threading/GIL issues.
 */
int32_t Producer_partitioner_cb (const rd_kafka_topic_t *rkt,
				 const void *keydata,
				 size_t keylen,
				 int32_t partition_cnt,
				 void *rkt_opaque, void *msg_opaque) {
	Producer *self = rkt_opaque;
	struct Producer_msgstate *msgstate = msg_opaque;
	PyGILState_STATE gstate;
	PyObject *result;
	PyObject *args;
	int32_t r = RD_KAFKA_PARTITION_UA;

	if (!msgstate) {
		/* Fall back on default C partitioner if neither a per-msg
		 * partitioner nor a default Python partitioner is available */
		return self->c_partitioner_cb(rkt, keydata, keylen,
					      partition_cnt, rkt_opaque,
					      msg_opaque);
	}

	gstate = PyGILState_Ensure();

	if (!msgstate->partitioner_cb) {
		/* Fall back on default C partitioner if neither a per-msg
		 * partitioner nor a default Python partitioner is available */
		r = msgstate->self->c_partitioner_cb(rkt, keydata, keylen,
						     partition_cnt, rkt_opaque,
						     msg_opaque);
		goto done;
	}

	args = Py_BuildValue("(s#l)",
			     (const char *)keydata, (int)keylen,
			     (long)partition_cnt);
	if (!args) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
				 "Unable to build callback args");
		printf("Failed to build args\n");
		goto done;
	}


	result = PyObject_CallObject(msgstate->partitioner_cb, args);
	Py_DECREF(args);

	if (result) {
		r = PyLong_AsLong(result);
		if (PyErr_Occurred())
			printf("FIXME: partition_cb returned wrong type "
			       "(expected long), how to propagate?\n");
		Py_DECREF(result);
	} else {
		printf("FIXME: partitioner_cb crashed, how to propagate?\n");
	}

 done:
	PyGILState_Release(gstate);
	return r;
}





static PyObject *Producer_produce (Producer *self, PyObject *args,
				       PyObject *kwargs) {
	const char *topic, *value = NULL, *key = NULL;
	int value_len = 0, key_len = 0;
	int partition = RD_KAFKA_PARTITION_UA;
	PyObject *dr_cb = NULL, *dr_cb2 = NULL, *partitioner_cb = NULL;
	rd_kafka_topic_t *rkt;
	struct Producer_msgstate *msgstate;
	static char *kws[] = { "topic",
			       "value",
			       "key",
			       "partition",
			       "callback",
			       "on_delivery", /* Alias */
			       "partitioner",
			       NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs,
					 "s|z#z#iOOO", kws,
					 &topic, &value, &value_len,
					 &key, &key_len, &partition,
					 &dr_cb, &dr_cb2, &partitioner_cb))
		return NULL;

	if (dr_cb2 && !dr_cb) /* Alias */
		dr_cb = dr_cb2;

	if (!(rkt = rd_kafka_topic_new(self->rk, topic, NULL))) {
		cfl_PyErr_Format(rd_kafka_last_error(),
				 "Unable to create topic object: %s",
				 rd_kafka_err2str(rd_kafka_last_error()));
		return NULL;
	}

	if (!dr_cb)
		dr_cb = self->default_dr_cb;
	if (!partitioner_cb)
		partitioner_cb = self->partitioner_cb;

	/* Create msgstate if necessary, may return NULL if no callbacks
	 * are wanted. */
	msgstate = Producer_msgstate_new(self, dr_cb, partitioner_cb);

	/* Produce message */
	if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			     (void *)value, value_len,
			     (void *)key, key_len, msgstate) == -1) {
		rd_kafka_resp_err_t err = rd_kafka_last_error();

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
	
	rd_kafka_topic_destroy(rkt);

	Py_RETURN_NONE;
}


/**
 * @brief Call rd_kafka_poll() and keep track of crashing callbacks.
 * @returns -1 if callback crashed (or poll() failed), else the number
 * of events served.
 */
static int Producer_poll0 (Producer *self, int tmout) {
	int r;

	self->callback_crashed = 0;
	self->thread_state = PyEval_SaveThread();

	r = rd_kafka_poll(self->rk, tmout);

	PyEval_RestoreThread(self->thread_state);
	self->thread_state = NULL;

	if (PyErr_CheckSignals() == -1)
		return -1;

	if (self->callback_crashed)
		return -1;

	return r;
}


static PyObject *Producer_poll (Producer *self, PyObject *args,
				    PyObject *kwargs) {
	double tmout;
	int r;
	static char *kws[] = { "timeout", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "d", kws, &tmout))
		return NULL;

	r = Producer_poll0(self, (int)(tmout * 1000));
	if (r == -1)
		return NULL;

	return PyLong_FromLong(r);
}


static PyObject *Producer_flush (Producer *self, PyObject *ignore) {
	while (rd_kafka_outq_len(self->rk) > 0) {
		if (Producer_poll0(self, 500) == -1)
			return NULL;
	}
	Py_RETURN_NONE;
}


static PyMethodDef Producer_methods[] = {
	{ "produce", (PyCFunction)Producer_produce,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: produce(topic, [value], [key], [partition], [callback])\n"
	  "\n"
	  "  Produce message to topic.\n"
	  "  This is an asynchronous operation, an application may use the "
	  "``callback`` (alias ``on_delivery``) argument to pass a function "
	  "(or lambda) that will be called from :py:func:`poll()` when the "
	  "message has been succesfully delivered or permanently fails delivery.\n"
	  "\n"
	  "  :param str topic: Topic to produce message to\n"
	  "  :param str|bytes value: Message payload\n"
	  "  :param str|bytes key: Message key\n"
	  "  :param int partition: Partition to produce to, elses uses the "
	  "configured partitioner.\n"
	  "  :param func on_delivery(err,msg): Delivery report callback to call "
	  "(from :py:func:`poll()` or :py:func:`flush()`) on succesful or "
	  "failed delivery\n"
	  "\n"
	  "  :rtype: None\n"
	  "  :raises BufferError: if the internal producer message queue is "
	  "full (``queue.buffering.max.messages`` exceeded)\n"
	  "  :raises KafkaException: for other errors, see exception code\n"
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
	  "  :param float timeout: Maximum time to block waiting for events.\n"
	  "  :returns: Number of events processed (callbacks served)\n"
	  "  :rtype: int\n"
	  "\n"
	},

	{ "flush", (PyCFunction)Producer_flush, METH_NOARGS,
	  "   Wait for all messages in the Producer queue to be delivered.\n"
	  "   This is a convenience method that calls :py:func:`poll()` until "
	  ":py:func:`len()` is zero.\n"
	  "\n"
	  ".. note:: See :py:func:`poll()` for a description on what "
	  "callbacks may be triggered.\n"
	  "\n"
	},
	{ NULL }
};


static Py_ssize_t Producer__len__ (Producer *self) {
	return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Producer_seq_methods = {
	(lenfunc)Producer__len__ /* sq_length */
};
	

static PyObject *Producer_new (PyTypeObject *type, PyObject *args,
				   PyObject *kwargs);

PyTypeObject ProducerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Producer",        /*tp_name*/
	sizeof(Producer),      /*tp_basicsize*/
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
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"Asynchronous Kafka Producer\n"
	"\n"
	".. py:function:: Producer(**kwargs)\n"
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
	0,                         /* tp_init */
	0,                         /* tp_alloc */
	Producer_new           /* tp_new */
};



static PyObject *Producer_new (PyTypeObject *type, PyObject *args,
				   PyObject *kwargs) {
	Producer *self;
	char errstr[256];
	rd_kafka_conf_t *conf;

	self = (Producer *)ProducerType.tp_alloc(&ProducerType, 0);
	if (!self)
		return NULL;

	if (!(conf = common_conf_setup(RD_KAFKA_PRODUCER, self,
					   args, kwargs))) {
		Py_DECREF(self);
		return NULL;
	}

	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

	self->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
				errstr, sizeof(errstr));
	if (!self->rk) {
		cfl_PyErr_Format(rd_kafka_last_error(),
				 "Failed to create producer: %s", errstr);
		Py_DECREF(self);
		return NULL;
	}

	return (PyObject *)self;
}



