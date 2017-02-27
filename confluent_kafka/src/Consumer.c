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


/****************************************************************************
 *
 *
 * Consumer
 *
 *
 *
 *
 ****************************************************************************/


static int Consumer_clear (Handle *self) {
	if (self->u.Consumer.on_assign) {
		Py_DECREF(self->u.Consumer.on_assign);
		self->u.Consumer.on_assign = NULL;
	}
	if (self->u.Consumer.on_revoke) {
		Py_DECREF(self->u.Consumer.on_revoke);
		self->u.Consumer.on_revoke = NULL;
	}
	if (self->u.Consumer.on_commit) {
		Py_DECREF(self->u.Consumer.on_commit);
		self->u.Consumer.on_commit = NULL;
	}

	Handle_clear(self);

	return 0;
}

static void Consumer_dealloc (Handle *self) {
	PyObject_GC_UnTrack(self);

	Consumer_clear(self);

	if (self->rk) {
                CallState cs;

                CallState_begin(self, &cs);

                /* If application has not called c.close() then
                 * rd_kafka_destroy() will, and that might trigger
                 * callbacks to be called from consumer_close().
                 * This should probably be fixed in librdkafka,
                 * or the application. */
                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Consumer_traverse (Handle *self,
			      visitproc visit, void *arg) {
	if (self->u.Consumer.on_assign)
		Py_VISIT(self->u.Consumer.on_assign);
	if (self->u.Consumer.on_revoke)
		Py_VISIT(self->u.Consumer.on_revoke);
	if (self->u.Consumer.on_commit)
		Py_VISIT(self->u.Consumer.on_commit);

	Handle_traverse(self, visit, arg);

	return 0;
}






static PyObject *Consumer_subscribe (Handle *self, PyObject *args,
					 PyObject *kwargs) {

	rd_kafka_topic_partition_list_t *topics;
	static char *kws[] = { "topics", "on_assign", "on_revoke", NULL };
	PyObject *tlist, *on_assign = NULL, *on_revoke = NULL;
	Py_ssize_t pos = 0;
	rd_kafka_resp_err_t err;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OO", kws,
					 &tlist, &on_assign, &on_revoke))
		return NULL;

	if (!PyList_Check(tlist)) {
		PyErr_Format(PyExc_TypeError,
			     "expected list of topic unicode strings");
		return NULL;
	}

	if (on_assign && !PyCallable_Check(on_assign)) {
		PyErr_Format(PyExc_TypeError,
			     "on_assign expects a callable");
		return NULL;
	}

	if (on_revoke && !PyCallable_Check(on_revoke)) {
		PyErr_Format(PyExc_TypeError,
			     "on_revoke expects a callable");
		return NULL;
	}

	topics = rd_kafka_topic_partition_list_new((int)PyList_Size(tlist));
	for (pos = 0 ; pos < PyList_Size(tlist) ; pos++) {
		PyObject *o = PyList_GetItem(tlist, pos);
		PyObject *uo;
		if (!(uo = cfl_PyObject_Unistr(o))) {
			PyErr_Format(PyExc_TypeError,
				     "expected list of unicode strings");
			rd_kafka_topic_partition_list_destroy(topics);
			return NULL;
		}
		rd_kafka_topic_partition_list_add(topics,
						  cfl_PyUnistr_AsUTF8(uo),
						  RD_KAFKA_PARTITION_UA);
		Py_DECREF(uo);
	}

	err = rd_kafka_subscribe(self->rk, topics);

	rd_kafka_topic_partition_list_destroy(topics);

	if (err) {
		cfl_PyErr_Format(err,
				 "Failed to set subscription: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}

	/*
	 * Update rebalance callbacks
	 */
	if (self->u.Consumer.on_assign) {
		Py_DECREF(self->u.Consumer.on_assign);
		self->u.Consumer.on_assign = NULL;
	}
	if (on_assign) {
		self->u.Consumer.on_assign = on_assign;
		Py_INCREF(self->u.Consumer.on_assign);
	}

	if (self->u.Consumer.on_revoke) {
		Py_DECREF(self->u.Consumer.on_revoke);
		self->u.Consumer.on_revoke = NULL;
	}
	if (on_revoke) {
		self->u.Consumer.on_revoke = on_revoke;
		Py_INCREF(self->u.Consumer.on_revoke);
	}

	Py_RETURN_NONE;
}


static PyObject *Consumer_unsubscribe (Handle *self,
					   PyObject *ignore) {

	rd_kafka_resp_err_t err;

	err = rd_kafka_unsubscribe(self->rk);
	if (err) {
		cfl_PyErr_Format(err,
				 "Failed to remove subscription: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}

	Py_RETURN_NONE;
}


static PyObject *Consumer_assign (Handle *self, PyObject *tlist) {

	rd_kafka_topic_partition_list_t *c_parts;
	rd_kafka_resp_err_t err;

	if (!(c_parts = py_to_c_parts(tlist)))
		return NULL;

	self->u.Consumer.rebalance_assigned++;

	err = rd_kafka_assign(self->rk, c_parts);

	rd_kafka_topic_partition_list_destroy(c_parts);

	if (err) {
		cfl_PyErr_Format(err,
				 "Failed to set assignment: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}

	Py_RETURN_NONE;
}


static PyObject *Consumer_unassign (Handle *self, PyObject *ignore) {

	rd_kafka_resp_err_t err;

	self->u.Consumer.rebalance_assigned++;

	err = rd_kafka_assign(self->rk, NULL);
	if (err) {
		cfl_PyErr_Format(err,
				 "Failed to remove assignment: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}

	Py_RETURN_NONE;
}

static PyObject *Consumer_assignment (Handle *self, PyObject *args,
                                      PyObject *kwargs) {

        PyObject *plist;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;

        err = rd_kafka_assignment(self->rk, &c_parts);
        if (err) {
                cfl_PyErr_Format(err,
                                 "Failed to get assignment: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }


        plist = c_parts_to_py(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);

        return plist;
}



static PyObject *Consumer_commit (Handle *self, PyObject *args,
					PyObject *kwargs) {

	rd_kafka_resp_err_t err;
	PyObject *msg = NULL, *offsets = NULL, *async_o = NULL;
	rd_kafka_topic_partition_list_t *c_offsets;
	int async = 1;
	static char *kws[] = { "message", "offsets", "async",NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOO", kws,
					 &msg, &offsets, &async_o))
		return NULL;

	if (msg && offsets) {
		PyErr_SetString(PyExc_ValueError,
				"message and offsets are mutually exclusive");
		return NULL;
	}

	if (async_o)
		async = PyObject_IsTrue(async_o);


	if (offsets) {

		if (!(c_offsets = py_to_c_parts(offsets)))
			return NULL;
	} else if (msg) {
		Message *m;

		if (PyObject_Type((PyObject *)msg) !=
		    (PyObject *)&MessageType) {
			PyErr_Format(PyExc_TypeError,
				     "expected %s", MessageType.tp_name);
			return NULL;
		}

		m = (Message *)msg;

		c_offsets = rd_kafka_topic_partition_list_new(1);
		rd_kafka_topic_partition_list_add(
			c_offsets, cfl_PyUnistr_AsUTF8(m->topic),
			m->partition)->offset =m->offset + 1;

	} else {
		c_offsets = NULL;
	}

			
	err = rd_kafka_commit(self->rk, c_offsets, async);

	if (c_offsets)
		rd_kafka_topic_partition_list_destroy(c_offsets);



	if (err) {
		cfl_PyErr_Format(err,
				 "Commit failed: %s", rd_kafka_err2str(err));
		return NULL;
	}

	Py_RETURN_NONE;
}



static PyObject *Consumer_committed (Handle *self, PyObject *args,
					 PyObject *kwargs) {

	PyObject *plist;
	rd_kafka_topic_partition_list_t *c_parts;
	rd_kafka_resp_err_t err;
	double tmout = -1.0f;
	static char *kws[] = { "partitions", "timeout", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|d", kws,
					 &plist, &tmout))
		return NULL;


	if (!(c_parts = py_to_c_parts(plist)))
		return NULL;

	err = rd_kafka_committed(self->rk, c_parts,
				tmout >= 0 ? (int)(tmout * 1000.0f) : -1);

	if (err) {
		rd_kafka_topic_partition_list_destroy(c_parts);
		cfl_PyErr_Format(err,
				 "Failed to get committed offsets: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}


	plist = c_parts_to_py(c_parts);
	rd_kafka_topic_partition_list_destroy(c_parts);

	return plist;
}


static PyObject *Consumer_position (Handle *self, PyObject *args,
					PyObject *kwargs) {

	PyObject *plist;
	rd_kafka_topic_partition_list_t *c_parts;
	rd_kafka_resp_err_t err;
	static char *kws[] = { "partitions", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kws,
					 &plist))
		return NULL;


	if (!(c_parts = py_to_c_parts(plist)))
		return NULL;

	err = rd_kafka_position(self->rk, c_parts);

	if (err) {
		rd_kafka_topic_partition_list_destroy(c_parts);
		cfl_PyErr_Format(err,
				 "Failed to get position: %s",
				 rd_kafka_err2str(err));
		return NULL;
	}


	plist = c_parts_to_py(c_parts);
	rd_kafka_topic_partition_list_destroy(c_parts);

	return plist;
}


static PyObject *Consumer_get_watermark_offsets (Handle *self, PyObject *args,
                                                 PyObject *kwargs) {

        TopicPartition *tp;
        rd_kafka_resp_err_t err;
        double tmout = -1.0f;
        int cached = 0;
        int64_t low = RD_KAFKA_OFFSET_INVALID, high = RD_KAFKA_OFFSET_INVALID;
        static char *kws[] = { "partition", "timeout", "cached", NULL };
        PyObject *rtup;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|db", kws,
                                         (PyObject **)&tp, &tmout, &cached))
                return NULL;


        if (PyObject_Type((PyObject *)tp) != (PyObject *)&TopicPartitionType) {
                PyErr_Format(PyExc_TypeError,
                             "expected %s", TopicPartitionType.tp_name);
                return NULL;
        }

        if (cached) {
                err = rd_kafka_get_watermark_offsets(self->rk,
                                                     tp->topic, tp->partition,
                                                     &low, &high);
        } else {
                err = rd_kafka_query_watermark_offsets(self->rk,
                                                       tp->topic, tp->partition,
                                                       &low, &high,
                                                       tmout >= 0 ? (int)(tmout * 1000.0f) : -1);
        }

        if (err) {
                cfl_PyErr_Format(err,
                                 "Failed to get watermark offsets: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        rtup = PyTuple_New(2);
        PyTuple_SetItem(rtup, 0, PyLong_FromLongLong(low));
        PyTuple_SetItem(rtup, 1, PyLong_FromLongLong(high));

        return rtup;
}



static PyObject *Consumer_poll (Handle *self, PyObject *args,
                                    PyObject *kwargs) {
        double tmout = -1.0f;
        static char *kws[] = { "timeout", NULL };
        rd_kafka_message_t *rkm;
        PyObject *msgobj;
        CallState cs;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

        CallState_begin(self, &cs);

        rkm = rd_kafka_consumer_poll(self->rk, tmout >= 0 ?
                                     (int)(tmout * 1000.0f) : -1);

        if (!CallState_end(self, &cs))
                return NULL;

        if (!rkm)
                Py_RETURN_NONE;

        msgobj = Message_new0(self, rkm);
        rd_kafka_message_destroy(rkm);

        return msgobj;
}


static PyObject *Consumer_close (Handle *self, PyObject *ignore) {
	CallState cs;

	CallState_begin(self, &cs);

	rd_kafka_consumer_close(self->rk);

	rd_kafka_destroy(self->rk);
	self->rk = NULL;

        if (!CallState_end(self, &cs))
                return NULL;

	Py_RETURN_NONE;
}


static PyMethodDef Consumer_methods[] = {
	{ "subscribe", (PyCFunction)Consumer_subscribe,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: subscribe(topics, [listener=None])\n"
	  "\n"
	  "  Set subscription to supplied list of topics\n"
	  "  This replaces a previous subscription.\n"
          "\n"
          "  Regexp pattern subscriptions are supported by prefixing "
          "the topic string with ``\"^\"``, e.g.::\n"
          "\n"
          "    consumer.subscribe([\"^my_topic.*\", \"^another[0-9]-?[a-z]+$\", \"not_a_regex\"])\n"
	  "\n"
	  "  :param list(str) topics: List of topics (strings) to subscribe to.\n"
	  "  :param callable on_assign: callback to provide handling of "
	  "customized offsets on completion of a successful partition "
	  "re-assignment.\n"
	  "  :param callable on_revoke: callback to provide handling of "
	  "offset commits to a customized store on the start of a "
	  "rebalance operation.\n"
	  "\n"
	  "  :raises KafkaException:\n"
	  "\n"
	  "\n"
	  ".. py:function:: on_assign(consumer, partitions)\n"
	  ".. py:function:: on_revoke(consumer, partitions)\n"
	  "\n"
	  "  :param Consumer consumer: Consumer instance.\n"
	  "  :param list(TopicPartition) partitions: Absolute list of partitions being assigned or revoked.\n"
	  "\n"
	},
        { "unsubscribe", (PyCFunction)Consumer_unsubscribe, METH_NOARGS,
          "  Remove current subscription.\n"
          "  :raises: KafkaException\n"
          "\n"
        },
	{ "poll", (PyCFunction)Consumer_poll,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: poll([timeout=None])\n"
	  "\n"
	  "  Consume messages, calls callbacks and returns events.\n"
	  "\n"
	  "  The application must check the returned :py:class:`Message` "
	  "object's :py:func:`Message.error()` method to distinguish "
	  "between proper messages (error() returns None), or an event or "
	  "error (see error().code() for specifics).\n"
	  "\n"
	  "  .. note: Callbacks may be called from this method, "
	  "such as ``on_assign``, ``on_revoke``, et.al.\n"
	  "\n"
	  "  :param float timeout: Maximum time to block waiting for message, event or callback.\n"
	  "  :returns: A Message object or None on timeout\n"
	  "  :rtype: :py:class:`Message` or None\n"
	  "\n"
	},
	{ "assign", (PyCFunction)Consumer_assign, METH_O,
	  ".. py:function:: assign(partitions)\n"
	  "\n"
	  "  Set consumer partition assignment to the provided list of "
	  ":py:class:`TopicPartition` and starts consuming.\n"
	  "\n"
	  "  :param list(TopicPartition) partitions: List of topic+partitions and optionally initial offsets to start consuming.\n"
	  "\n"
	},
        { "unassign", (PyCFunction)Consumer_unassign, METH_NOARGS,
          "  Removes the current partition assignment and stops consuming.\n"
          "  :raises: KafkaException\n"
          "\n"
        },
        { "assignment", (PyCFunction)Consumer_assignment,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: assignment()\n"
          "\n"
          "  Returns the current partition assignment.\n"
          "\n"
          "  :returns: List of assigned topic+partitions.\n"
          "  :rtype: list(TopicPartition)\n"
          "  :raises: KafkaException\n"
          "\n"
        },
	{ "commit", (PyCFunction)Consumer_commit, METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: commit([message=None], [offsets=None], [async=True])\n"
	  "\n"
	  "  Commit a message or a list of offsets.\n"
	  "\n"
	  "  ``message`` and ``offsets`` are mutually exclusive, if neither is set "
	  "the current partition assignment's offsets are used instead. "
	  "The consumer relies on your use of this method if you have set 'enable.auto.commit' to False\n"
	  "\n"
	  "  :param confluent_kafka.Message message: Commit message's offset+1.\n"
	  "  :param list(TopicPartition) offsets: List of topic+partitions+offsets to commit.\n"
	  "  :param bool async: Asynchronous commit, return immediately.\n"
	  "  :rtype: None\n"
	  "  :raises: KafkaException\n"
	  "\n"
	},	  
	{ "committed", (PyCFunction)Consumer_committed,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: committed(partitions, [timeout=None])\n"
	  "\n"
	  "  Retrieve committed offsets for the list of partitions.\n"
	  "\n"
	  "  :param list(TopicPartition) partitions: List of topic+partitions "
	  "to query for stored offsets.\n"
	  "  :param float timeout: Request timeout\n"
	  "  :returns: List of topic+partitions with offset and possibly error set.\n"
	  "  :rtype: list(TopicPartition)\n"
	  "  :raises: KafkaException\n"
	  "\n"
	},
	{ "position", (PyCFunction)Consumer_position,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: position(partitions, [timeout=None])\n"
	  "\n"
	  "  Retrieve current positions (offsets) for the list of partitions.\n"
	  "\n"
	  "  :param list(TopicPartition) partitions: List of topic+partitions "
	  "to return current offsets for. The current offset is the offset of the "
	  "last consumed message + 1.\n"
	  "  :returns: List of topic+partitions with offset and possibly error set.\n"
	  "  :rtype: list(TopicPartition)\n"
	  "  :raises: KafkaException\n"
	  "\n"
	},
        { "get_watermark_offsets", (PyCFunction)Consumer_get_watermark_offsets,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: get_watermark_offsets(partition, [timeout=None], [cached=False])\n"
          "\n"
          "  Retrieve low and high offsets for partition.\n"
          "\n"
          "  :param TopicPartition partition: Topic+partition to return offsets for."
          "  :param float timeout: Request timeout (when cached=False).\n"
          "  :param bool cached: Instead of querying the broker used cached information. "
          "Cached values: The low offset is updated periodically (if statistics.interval.ms is set) while "
          "the high offset is updated on each message fetched from the broker for this partition."
          "  :returns: Tuple of (low,high) on success or None on timeout.\n"
          "  :rtype: tuple(int,int)\n"
          "  :raises: KafkaException\n"
          "\n"
        },
	{ "close", (PyCFunction)Consumer_close, METH_NOARGS,
	  "\n"
	  "  Close down and terminate the Kafka Consumer.\n"
	  "\n"
	  "  Actions performed:\n"
	  "\n"
	  "  - Stops consuming\n"
	  "  - Commits offsets - except if the consumer property 'enable.auto.commit' is set to False\n"
	  "  - Leave consumer group\n"
	  "\n"
	  "  .. note: Registered callbacks may be called from this method, "
	  "see :py:func::`poll()` for more info.\n"
	  "\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ NULL }
};


static void Consumer_rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
				   rd_kafka_topic_partition_list_t *c_parts,
				   void *opaque) {
	Handle *self = opaque;
	CallState *cs;

	cs = CallState_get(self);

	self->u.Consumer.rebalance_assigned = 0;

	if ((err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS &&
	     self->u.Consumer.on_assign) ||
	    (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS &&
	     self->u.Consumer.on_revoke)) {
		PyObject *parts;
		PyObject *args, *result;

		/* Construct list of TopicPartition based on 'c_parts' */
		parts = c_parts_to_py(c_parts);

		args = Py_BuildValue("(OO)", self, parts);

		Py_DECREF(parts);

		if (!args) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
					 "Unable to build callback args");
			CallState_crash(cs);
			CallState_resume(cs);
			return;
		}

		result = PyObject_CallObject(
			err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ?
			self->u.Consumer.on_assign :
			self->u.Consumer.on_revoke, args);

		Py_DECREF(args);

		if (result)
			Py_DECREF(result);
		else {
			CallState_crash(cs);
			rd_kafka_yield(rk);
		}
	}

	/* Fallback: librdkafka needs the rebalance_cb to call assign()
	 * to synchronize state, if the user did not do this from callback,
	 * or there was no callback, or the callback failed, then we perform
	 * that assign() call here instead. */
	if (!self->u.Consumer.rebalance_assigned) {
		if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
			rd_kafka_assign(rk, c_parts);
		else
			rd_kafka_assign(rk, NULL);
	}

	CallState_resume(cs);
}


static void Consumer_offset_commit_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
				       rd_kafka_topic_partition_list_t *c_parts,
				       void *opaque) {
	Handle *self = opaque;
	PyObject *parts, *k_err, *args, *result;
	CallState *cs;

	if (!self->u.Consumer.on_commit)
		return;

	cs = CallState_get(self);

	/* Insantiate error object */
	k_err = KafkaError_new_or_None(err, NULL);

	/* Construct list of TopicPartition based on 'c_parts' */
	if (c_parts)
		parts = c_parts_to_py(c_parts);
	else
		parts = PyList_New(0);

	args = Py_BuildValue("(OO)", k_err, parts);

	Py_DECREF(k_err);
	Py_DECREF(parts);

	if (!args) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
				 "Unable to build callback args");
		CallState_crash(cs);
		CallState_resume(cs);
		return;
	}

	result = PyObject_CallObject(self->u.Consumer.on_commit, args);

	Py_DECREF(args);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(rk);
	}

	CallState_resume(cs);
}



static int Consumer_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Consumer already __init__:ialized");
                return -1;
        }

        self->type = RD_KAFKA_CONSUMER;

        if (!(conf = common_conf_setup(RD_KAFKA_CONSUMER, self,
                                       args, kwargs)))
                return -1; /* Exception raised by ..conf_setup() */

        rd_kafka_conf_set_rebalance_cb(conf, Consumer_rebalance_cb);
        rd_kafka_conf_set_offset_commit_cb(conf, Consumer_offset_commit_cb);

        self->rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                                errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create consumer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        rd_kafka_poll_set_consumer(self->rk);

        return 0;
}

static PyObject *Consumer_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}


PyTypeObject ConsumerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Consumer",        /*tp_name*/
	sizeof(Handle),          /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)Consumer_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"High-level Kafka Consumer\n"
	"\n"
	".. py:function:: Consumer(**kwargs)\n"
	"\n"
	"  Create new Consumer instance using provided configuration dict.\n"
	"\n"
	" Special configuration properties:\n"
	"   ``on_commit``: Optional callback will be called when a commit "
	"request has succeeded or failed.\n"
	"\n"
	"\n"
	".. py:function:: on_commit(err, partitions)\n"
	"\n"
	"  :param Consumer consumer: Consumer instance.\n"
	"  :param KafkaError err: Commit error object, or None on success.\n"
	"  :param list(TopicPartition) partitions: List of partitions with "
	"their committed offsets or per-partition errors.\n"
	"\n"
	"\n", /*tp_doc*/
	(traverseproc)Consumer_traverse, /* tp_traverse */
	(inquiry)Consumer_clear, /* tp_clear */
	0,		           /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	Consumer_methods,      /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
        Consumer_init,             /* tp_init */
	0,                         /* tp_alloc */
	Consumer_new           /* tp_new */
};
