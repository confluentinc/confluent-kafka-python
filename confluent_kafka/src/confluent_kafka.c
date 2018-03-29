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

#include <stdarg.h>


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
 *  - KafkaError type .tp_doc allocation is lost on exit.
 *
 */


PyObject *KafkaException;


/****************************************************************************
 *
 *
 * KafkaError
 *
 *
 * FIXME: Pre-create simple instances for each error code, only instantiate
 *        a new object if a rich error string is provided.
 *
 ****************************************************************************/
typedef struct {
	PyObject_HEAD
	rd_kafka_resp_err_t code;  /* Error code */
	char *str;   /* Human readable representation of error, if one
		      * was provided by librdkafka.
		      * Else falls back on err2str(). */
} KafkaError;


static PyObject *KafkaError_code (KafkaError *self, PyObject *ignore) {
	return PyLong_FromLong(self->code);
}

static PyObject *KafkaError_str (KafkaError *self, PyObject *ignore) {
	if (self->str)
		return cfl_PyUnistr(_FromString(self->str));
	else
		return cfl_PyUnistr(_FromString(rd_kafka_err2str(self->code)));
}

static PyObject *KafkaError_name (KafkaError *self, PyObject *ignore) {
	/* FIXME: Pre-create name objects */
	return cfl_PyUnistr(_FromString(rd_kafka_err2name(self->code)));
}


static PyMethodDef KafkaError_methods[] = {
	{ "code", (PyCFunction)KafkaError_code, METH_NOARGS,
	  "  Returns the error/event code for comparison to"
	  "KafkaError.<ERR_CONSTANTS>.\n"
	  "\n"
	  "  :returns: error/event code\n"
	  "  :rtype: int\n"
	  "\n"
	},
	{ "str", (PyCFunction)KafkaError_str, METH_NOARGS,
	  "  Returns the human-readable error/event string.\n"
	  "\n"
	  "  :returns: error/event message string\n"
	  "  :rtype: str\n"
	  "\n"
	},
	{ "name", (PyCFunction)KafkaError_name, METH_NOARGS,
	  "  Returns the enum name for error/event.\n"
	  "\n"
	  "  :returns: error/event enum name string\n"
	  "  :rtype: str\n"
	  "\n"
	},

	{ NULL }
};


static void KafkaError_dealloc (KafkaError *self) {
	if (self->str)
		free(self->str);
	Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyObject *KafkaError_str0 (KafkaError *self) {
	return cfl_PyUnistr(_FromFormat("KafkaError{code=%s,val=%d,str=\"%s\"}",
					 rd_kafka_err2name(self->code),
					 self->code,
					 self->str ? self->str :
					 rd_kafka_err2str(self->code)));
}

static long KafkaError_hash (KafkaError *self) {
	return self->code;
}

static PyTypeObject KafkaErrorType;

static PyObject* KafkaError_richcompare (KafkaError *self, PyObject *o2,
					 int op) {
	int code2;
	int r;
	PyObject *result;

	if (Py_TYPE(o2) == &KafkaErrorType)
		code2 = ((KafkaError *)o2)->code;
	else
		code2 = (int)PyLong_AsLong(o2);

	switch (op)
	{
	case Py_LT:
		r = self->code < code2;
		break;
	case Py_LE:
		r = self->code <= code2;
		break;
	case Py_EQ:
		r = self->code == code2;
		break;
	case Py_NE:
		r = self->code != code2;
		break;
	case Py_GT:
		r = self->code > code2;
		break;
	case Py_GE:
		r = self->code >= code2;
		break;
	default:
		r = 0;
		break;
	}

	result = r ? Py_True : Py_False;
	Py_INCREF(result);
	return result;
}


static PyTypeObject KafkaErrorType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.KafkaError",      /*tp_name*/
	sizeof(KafkaError),    /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)KafkaError_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	(reprfunc)KafkaError_str0, /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	(hashfunc)KafkaError_hash, /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	PyObject_GenericGetAttr,   /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
	"Kafka error and event object\n"
	"\n"
	"  The KafkaError class serves multiple purposes:\n"
	"\n"
	"  - Propagation of errors\n"
	"  - Propagation of events\n"
	"  - Exceptions\n"
	"\n"
	"  This class is not user-instantiable.\n"
	"\n", /*tp_doc*/
	0,		           /* tp_traverse */
	0,		           /* tp_clear */
	(richcmpfunc)KafkaError_richcompare, /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	KafkaError_methods,    /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	0,                         /* tp_init */
	0                          /* tp_alloc */
};


/**
 * @brief Initialize a KafkaError object.
 */
static void KafkaError_init (KafkaError *self,
			     rd_kafka_resp_err_t code, const char *str) {
	self->code = code;
	if (str)
		self->str = strdup(str);
	else
		self->str = NULL;
}

/**
 * @brief Internal factory to create KafkaError object.
 */
PyObject *KafkaError_new0 (rd_kafka_resp_err_t err, const char *fmt, ...) {

	KafkaError *self;
	va_list ap;
	char buf[512];

	self = (KafkaError *)KafkaErrorType.
		tp_alloc(&KafkaErrorType, 0);
	if (!self)
		return NULL;

	if (fmt) {
		va_start(ap, fmt);
		vsnprintf(buf, sizeof(buf), fmt, ap);
		va_end(ap);
	}

	KafkaError_init(self, err, fmt ? buf : rd_kafka_err2str(err));

	return (PyObject *)self;
}

/**
 * @brief Internal factory to create KafkaError object.
 * @returns a new KafkaError object if \p err != 0, else a None object.
 */
 PyObject *KafkaError_new_or_None (rd_kafka_resp_err_t err, const char *str) {
	if (!err)
		Py_RETURN_NONE;
        if (str)
                return KafkaError_new0(err, "%s", str);
        else
                return KafkaError_new0(err, NULL);
}




/****************************************************************************
 *
 *
 * Message
 *
 *
 *
 *
 ****************************************************************************/


PyObject *Message_error (Message *self, PyObject *ignore) {
	if (self->error) {
		Py_INCREF(self->error);
		return self->error;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_value (Message *self, PyObject *ignore) {
	if (self->value) {
		Py_INCREF(self->value);
		return self->value;
	} else
		Py_RETURN_NONE;
}


static PyObject *Message_key (Message *self, PyObject *ignore) {
	if (self->key) {
		Py_INCREF(self->key);
		return self->key;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_topic (Message *self, PyObject *ignore) {
	if (self->topic) {
		Py_INCREF(self->topic);
		return self->topic;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_partition (Message *self, PyObject *ignore) {
	if (self->partition != RD_KAFKA_PARTITION_UA)
		return PyLong_FromLong(self->partition);
	else
		Py_RETURN_NONE;
}


static PyObject *Message_offset (Message *self, PyObject *ignore) {
	if (self->offset >= 0)
		return PyLong_FromLongLong(self->offset);
	else
		Py_RETURN_NONE;
}


static PyObject *Message_timestamp (Message *self, PyObject *ignore) {
	return Py_BuildValue("iL",
			     self->tstype,
			     self->timestamp);
}

static PyObject *Message_headers (Message *self, PyObject *ignore) {
#ifdef RD_KAFKA_V_HEADERS
	if (self->headers) {
        Py_INCREF(self->headers);
		return self->headers;
    } else if (self->c_headers) {
        self->headers = c_headers_to_py(self->c_headers);
        rd_kafka_headers_destroy(self->c_headers);
        self->c_headers = NULL;
        Py_INCREF(self->headers);
        return self->headers;
	} else {
		Py_RETURN_NONE;
    }
#else
		Py_RETURN_NONE;
#endif
}

static PyObject *Message_set_headers (Message *self, PyObject *new_headers) {
   if (self->headers)
        Py_DECREF(self->headers);
   self->headers = new_headers;
   Py_INCREF(self->headers);

   Py_RETURN_NONE;
}

static PyObject *Message_set_value (Message *self, PyObject *new_val) {
   if (self->value)
        Py_DECREF(self->value);
   self->value = new_val;
   Py_INCREF(self->value);

   Py_RETURN_NONE;
}

static PyObject *Message_set_key (Message *self, PyObject *new_key) {
   if (self->key)
        Py_DECREF(self->key);
   self->key = new_key;
   Py_INCREF(self->key);

   Py_RETURN_NONE;
}

static PyMethodDef Message_methods[] = {
	{ "error", (PyCFunction)Message_error, METH_NOARGS,
	  "  The message object is also used to propagate errors and events, "
	  "an application must check error() to determine if the Message "
	  "is a proper message (error() returns None) or an error or event "
	  "(error() returns a KafkaError object)\n"
	  "\n"
	  "  :rtype: None or :py:class:`KafkaError`\n"
	  "\n"
	},

	{ "value", (PyCFunction)Message_value, METH_NOARGS,
	  "  :returns: message value (payload) or None if not available.\n"
	  "  :rtype: str|bytes or None\n"
	  "\n"
	},
	{ "key", (PyCFunction)Message_key, METH_NOARGS,
	  "  :returns: message key or None if not available.\n"
	  "  :rtype: str|bytes or None\n"
	  "\n"
	},
	{ "topic", (PyCFunction)Message_topic, METH_NOARGS,
	  "  :returns: topic name or None if not available.\n"
	  "  :rtype: str or None\n"
	  "\n"
	},
	{ "partition", (PyCFunction)Message_partition, METH_NOARGS,
	  "  :returns: partition number or None if not available.\n"
	  "  :rtype: int or None\n"
	  "\n"
	},
	{ "offset", (PyCFunction)Message_offset, METH_NOARGS,
	  "  :returns: message offset or None if not available.\n"
	  "  :rtype: int or None\n"
	  "\n"
	},
	{ "timestamp", (PyCFunction)Message_timestamp, METH_NOARGS,
          "  Retrieve timestamp type and timestamp from message.\n"
          "  The timestamp type is one of:\n"
          "    * :py:const:`TIMESTAMP_NOT_AVAILABLE`"
          " - Timestamps not supported by broker\n"
          "    * :py:const:`TIMESTAMP_CREATE_TIME` "
          " - Message creation time (or source / producer time)\n"
          "    * :py:const:`TIMESTAMP_LOG_APPEND_TIME` "
          " - Broker receive time\n"
          "\n"
          "  The returned timestamp should be ignored if the timestamp type is "
          ":py:const:`TIMESTAMP_NOT_AVAILABLE`.\n"
          "\n"
          "  The timestamp is the number of milliseconds since the epoch (UTC).\n"
          "\n"
          "  Timestamps require broker version 0.10.0.0 or later and \n"
          "  ``{'api.version.request': True}`` configured on the client.\n"
          "\n"
	  "  :returns: tuple of message timestamp type, and timestamp.\n"
	  "  :rtype: (int, int)\n"
	  "\n"
	},
	{ "headers", (PyCFunction)Message_headers, METH_NOARGS,
      "  Retrieve the headers set on a message. Each header is a key value"
      "pair. Please note that header keys are ordered and can repeat.\n"
      "\n"
	  "  :returns: list of two-tuples, one (key, value) pair for each header.\n"
	  "  :rtype: [(str, bytes),...] or None.\n"
	  "\n"
	},
	{ "set_headers", (PyCFunction)Message_set_headers, METH_O,
	  "  Set the field 'Message.headers' with new value.\n"
	  "  :param: object value: Message.headers.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ "set_value", (PyCFunction)Message_set_value, METH_O,
	  "  Set the field 'Message.value' with new value.\n"
	  "  :param: object value: Message.value.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ "set_key", (PyCFunction)Message_set_key, METH_O,
	  "  Set the field 'Message.key' with new value.\n"
	  "  :param: object value: Message.key.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ NULL }
};

static int Message_clear (Message *self) {
	if (self->topic) {
		Py_DECREF(self->topic);
		self->topic = NULL;
	}
	if (self->value) {
		Py_DECREF(self->value);
		self->value = NULL;
	}
	if (self->key) {
		Py_DECREF(self->key);
		self->key = NULL;
	}
	if (self->error) {
		Py_DECREF(self->error);
		self->error = NULL;
	}
	if (self->headers) {
		Py_DECREF(self->headers);
		self->headers = NULL;
	}
#ifdef RD_KAFKA_V_HEADERS
    if (self->c_headers){
        rd_kafka_headers_destroy(self->c_headers);
        self->c_headers = NULL;
    }
#endif
	return 0;
}


static void Message_dealloc (Message *self) {
	Message_clear(self);
	PyObject_GC_UnTrack(self);
	Py_TYPE(self)->tp_free((PyObject *)self);
}


static int Message_traverse (Message *self,
			     visitproc visit, void *arg) {
	if (self->topic)
		Py_VISIT(self->topic);
	if (self->value)
		Py_VISIT(self->value);
	if (self->key)
		Py_VISIT(self->key);
	if (self->error)
		Py_VISIT(self->error);
	if (self->headers)
		Py_VISIT(self->headers);
	return 0;
}

static Py_ssize_t Message__len__ (Message *self) {
	return self->value ? PyObject_Length(self->value) : 0;
}

static PySequenceMethods Message_seq_methods = {
	(lenfunc)Message__len__ /* sq_length */
};

PyTypeObject MessageType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Message",         /*tp_name*/
	sizeof(Message),       /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)Message_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	0,                         /*tp_as_number*/
	&Message_seq_methods,  /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"The Message object represents either a single consumed or "
	"produced message, or an event (:py:func:`error()` is not None).\n"
	"\n"
	"An application must check with :py:func:`error()` to see if the "
	"object is a proper message (error() returns None) or an "
	"error/event.\n"
        "\n"
	"This class is not user-instantiable.\n"
	"\n"
	".. py:function:: len()\n"
	"\n"
	"  :returns: Message value (payload) size in bytes\n"
	"  :rtype: int\n"
	"\n", /*tp_doc*/
	(traverseproc)Message_traverse,        /* tp_traverse */
	(inquiry)Message_clear,	           /* tp_clear */
	0,		           /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	Message_methods,       /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	0,                         /* tp_init */
	0                          /* tp_alloc */
};

/**
 * @brief Internal factory to create Message object from message_t
 */
PyObject *Message_new0 (const Handle *handle, const rd_kafka_message_t *rkm) {
	Message *self;

	self = (Message *)MessageType.tp_alloc(&MessageType, 0);
	if (!self)
		return NULL;

        /* Only use message error string on Consumer, for Producers
         * it will contain the original message payload. */
        self->error = KafkaError_new_or_None(
                rkm->err,
                (rkm->err && handle->type != RD_KAFKA_PRODUCER) ?
                rd_kafka_message_errstr(rkm) : NULL);

	if (rkm->rkt)
		self->topic = cfl_PyUnistr(
			_FromString(rd_kafka_topic_name(rkm->rkt)));
	if (rkm->payload)
		self->value = cfl_PyBin(_FromStringAndSize(rkm->payload,
							   rkm->len));
	if (rkm->key)
		self->key = cfl_PyBin(
			_FromStringAndSize(rkm->key, rkm->key_len));

	self->partition = rkm->partition;
	self->offset = rkm->offset;

	self->timestamp = rd_kafka_message_timestamp(rkm, &self->tstype);

	return (PyObject *)self;
}




/****************************************************************************
 *
 *
 * TopicPartition
 *
 *
 *
 *
 ****************************************************************************/
static int TopicPartition_clear (TopicPartition *self) {
	if (self->topic) {
		free(self->topic);
		self->topic = NULL;
	}
	if (self->error) {
		Py_DECREF(self->error);
		self->error = NULL;
	}
	return 0;
}

static void TopicPartition_setup (TopicPartition *self, const char *topic,
				  int partition, long long offset,
				  rd_kafka_resp_err_t err) {
	self->topic = strdup(topic);
	self->partition = partition;
	self->offset = offset;
	self->error = KafkaError_new_or_None(err, NULL);
}


static void TopicPartition_dealloc (TopicPartition *self) {
	PyObject_GC_UnTrack(self);

	TopicPartition_clear(self);

	Py_TYPE(self)->tp_free((PyObject *)self);
}


static int TopicPartition_init (PyObject *self, PyObject *args,
				      PyObject *kwargs) {
	const char *topic;
	int partition = RD_KAFKA_PARTITION_UA;
	long long offset = RD_KAFKA_OFFSET_INVALID;
	static char *kws[] = { "topic",
			       "partition",
			       "offset",
			       NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|iL", kws,
					 &topic, &partition, &offset))
		return -1;

	TopicPartition_setup((TopicPartition *)self,
			     topic, partition, offset, 0);

	return 0;
}


static PyObject *TopicPartition_new (PyTypeObject *type, PyObject *args,
				     PyObject *kwargs) {
	PyObject *self = type->tp_alloc(type, 1);
	return self;
}



static int TopicPartition_traverse (TopicPartition *self,
				    visitproc visit, void *arg) {
	if (self->error)
		Py_VISIT(self->error);
	return 0;
}


static PyMemberDef TopicPartition_members[] = {
        { "topic", T_STRING, offsetof(TopicPartition, topic), READONLY,
          ":py:attribute:topic - Topic name (string)" },
        { "partition", T_INT, offsetof(TopicPartition, partition), 0,
          ":py:attribute: Partition number (int)" },
        { "offset", T_LONGLONG, offsetof(TopicPartition, offset), 0,
          " :py:attribute: Offset (long)\n"
          "Either an absolute offset (>=0) or a logical offset:"
          " :py:const:`OFFSET_BEGINNING`,"
          " :py:const:`OFFSET_END`,"
          " :py:const:`OFFSET_STORED`,"
          " :py:const:`OFFSET_INVALID`"
        },
        { "error", T_OBJECT, offsetof(TopicPartition, error), READONLY,
          ":py:attribute: Indicates an error (with :py:class:`KafkaError`) unless None." },
        { NULL }
};


static PyObject *TopicPartition_str0 (TopicPartition *self) {
        PyObject *errstr = NULL;
        PyObject *errstr8 = NULL;
        const char *c_errstr = NULL;
	PyObject *ret;
	char offset_str[40];

	snprintf(offset_str, sizeof(offset_str), "%"PRId64"", self->offset);

        if (self->error != Py_None) {
                errstr = cfl_PyObject_Unistr(self->error);
                c_errstr = cfl_PyUnistr_AsUTF8(errstr, &errstr8);
        }

	ret = cfl_PyUnistr(
		_FromFormat("TopicPartition{topic=%s,partition=%"PRId32
			    ",offset=%s,error=%s}",
			    self->topic, self->partition,
			    offset_str,
			    c_errstr ? c_errstr : "None"));
        Py_XDECREF(errstr8);
        Py_XDECREF(errstr);
	return ret;
}


static PyObject *
TopicPartition_richcompare (TopicPartition *self, PyObject *o2,
			    int op) {
	TopicPartition *a = self, *b;
	int tr, pr;
	int r;
	PyObject *result;

	if (Py_TYPE(o2) != Py_TYPE(self)) {
		PyErr_SetNone(PyExc_NotImplementedError);
		return NULL;
	}

	b = (TopicPartition *)o2;

	tr = strcmp(a->topic, b->topic);
	pr = a->partition - b->partition;
	switch (op)
	{
	case Py_LT:
		r = tr < 0 || (tr == 0 && pr < 0);
		break;
	case Py_LE:
		r = tr < 0 || (tr == 0 && pr <= 0);
		break;
	case Py_EQ:
		r = (tr == 0 && pr == 0);
		break;
	case Py_NE:
		r = (tr != 0 || pr != 0);
		break;
	case Py_GT:
		r = tr > 0 || (tr == 0 && pr > 0);
		break;
	case Py_GE:
		r = tr > 0 || (tr == 0 && pr >= 0);
		break;
	default:
		r = 0;
		break;
	}

	result = r ? Py_True : Py_False;
	Py_INCREF(result);
	return result;
}


static long TopicPartition_hash (TopicPartition *self) {
	PyObject *topic = cfl_PyUnistr(_FromString(self->topic));
	long r = PyObject_Hash(topic) ^ self->partition;
	Py_DECREF(topic);
	return r;
}


PyTypeObject TopicPartitionType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.TopicPartition",         /*tp_name*/
	sizeof(TopicPartition),       /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)TopicPartition_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	(reprfunc)TopicPartition_str0, /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	(hashfunc)TopicPartition_hash, /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	PyObject_GenericGetAttr,   /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"TopicPartition is a generic type to hold a single partition and "
	"various information about it.\n"
	"\n"
	"It is typically used to provide a list of topics or partitions for "
	"various operations, such as :py:func:`Consumer.assign()`.\n"
	"\n"
	".. py:function:: TopicPartition(topic, [partition], [offset])\n"
	"\n"
	"  Instantiate a TopicPartition object.\n"
	"\n"
	"  :param string topic: Topic name\n"
	"  :param int partition: Partition id\n"
	"  :param int offset: Initial partition offset\n"
	"  :rtype: TopicPartition\n"
	"\n"
	"\n", /*tp_doc*/
	(traverseproc)TopicPartition_traverse, /* tp_traverse */
	(inquiry)TopicPartition_clear,       /* tp_clear */
	(richcmpfunc)TopicPartition_richcompare, /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	0,                         /* tp_methods */
	TopicPartition_members,/* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	TopicPartition_init,       /* tp_init */
	0,                         /* tp_alloc */
	TopicPartition_new         /* tp_new */
};

/**
 * @brief Internal factory to create a TopicPartition object.
 */
static PyObject *TopicPartition_new0 (const char *topic, int partition,
				      long long offset,
				      rd_kafka_resp_err_t err) {
	TopicPartition *self;

	self = (TopicPartition *)TopicPartitionType.tp_new(
		&TopicPartitionType, NULL, NULL);

	TopicPartition_setup(self, topic, partition, offset, err);

	return (PyObject *)self;
}




/**
 * @brief Convert C rd_kafka_topic_partition_list_t to Python list(TopicPartition).
 *
 * @returns The new Python list object.
 */
PyObject *c_parts_to_py (const rd_kafka_topic_partition_list_t *c_parts) {
	PyObject *parts;
	size_t i;

	parts = PyList_New(c_parts->cnt);

	for (i = 0 ; i < c_parts->cnt ; i++) {
		const rd_kafka_topic_partition_t *rktpar = &c_parts->elems[i];
		PyList_SET_ITEM(parts, i,
				TopicPartition_new0(
					rktpar->topic, rktpar->partition,
					rktpar->offset, rktpar->err));
	}

	return parts;

}

/**
 * @brief Convert Python list(TopicPartition) to C rd_kafka_topic_partition_list_t.
 *
 * @returns The new C list on success or NULL on error.
 */
rd_kafka_topic_partition_list_t *py_to_c_parts (PyObject *plist) {
	rd_kafka_topic_partition_list_t *c_parts;
	size_t i;

	if (!PyList_Check(plist)) {
		PyErr_SetString(PyExc_TypeError,
				"requires list of TopicPartition");
		return NULL;
	}

	c_parts = rd_kafka_topic_partition_list_new((int)PyList_Size(plist));

	for (i = 0 ; i < PyList_Size(plist) ; i++) {
		TopicPartition *tp = (TopicPartition *)
			PyList_GetItem(plist, i);

		if (PyObject_Type((PyObject *)tp) !=
		    (PyObject *)&TopicPartitionType) {
			PyErr_Format(PyExc_TypeError,
				     "expected %s",
				     TopicPartitionType.tp_name);
			rd_kafka_topic_partition_list_destroy(c_parts);
			return NULL;
		}

		rd_kafka_topic_partition_list_add(c_parts,
						  tp->topic,
						  tp->partition)->offset =
			tp->offset;
	}

	return c_parts;
}

#ifdef RD_KAFKA_V_HEADERS
/**
 * @brief Convert Python list[(header_key, header_value),...]) to C rd_kafka_topic_partition_list_t.
 *
 * @returns The new Python list[(header_key, header_value),...] object.
 */
rd_kafka_headers_t *py_headers_to_c (PyObject *headers_plist) {
    int i, len;
    rd_kafka_headers_t *rd_headers = NULL;
    rd_kafka_resp_err_t err;
    const char *header_key, *header_value = NULL;
    int header_key_len = 0, header_value_len = 0;

    if (!PyList_Check(headers_plist)) {
            PyErr_SetString(PyExc_TypeError,
                            "Headers are expected to be a "
                            "list of (key,value) tuples");
            return NULL;
    }

    len = PyList_Size(headers_plist);
    rd_headers = rd_kafka_headers_new(len);

    for (i = 0; i < len; i++) {
        if(!PyArg_ParseTuple(PyList_GET_ITEM(headers_plist, i), "s#z#", &header_key,
                &header_key_len, &header_value, &header_value_len)){
            rd_kafka_headers_destroy(rd_headers);
            PyErr_SetString(PyExc_TypeError,
                    "Headers are expected to be a list of (key,value) tuples");
            return NULL;
        }

        err = rd_kafka_header_add(rd_headers, header_key, header_key_len, header_value, header_value_len);
        if (err) {
            rd_kafka_headers_destroy(rd_headers);
            cfl_PyErr_Format(err,
                     "Unable to create message headers: %s",
                     rd_kafka_err2str(err));
            return NULL;
        }
    }
    return rd_headers;
}

/**
 * @brief Convert rd_kafka_headers_t to Python list[(header_key, header_value),...])
 *
 * @returns The new C headers on success or NULL on error.
 */
PyObject *c_headers_to_py (rd_kafka_headers_t *headers) {
    size_t idx = 0;
    size_t header_size = 0;
    const char *header_key;
    const void *header_value;
    size_t header_value_size;
    PyObject *header_list;

    header_size = rd_kafka_header_cnt(headers); 
    header_list = PyList_New(header_size);

    while (!rd_kafka_header_get_all(headers, idx++,
                                     &header_key, &header_value, &header_value_size)) {
            // Create one (key, value) tuple for each header
            PyObject *header_tuple = PyTuple_New(2);
            PyTuple_SetItem(header_tuple, 0,
                cfl_PyUnistr(_FromString(header_key))
            );

            if (header_value) {
                    PyTuple_SetItem(header_tuple, 1,
                        cfl_PyBin(_FromStringAndSize(header_value, header_value_size))
                    );
            } else {
                PyTuple_SetItem(header_tuple, 1, Py_None);
            }
        PyList_SET_ITEM(header_list, idx-1, header_tuple);
    }

    return header_list;
}
#endif

/****************************************************************************
 *
 *
 * Common callbacks
 *
 *
 *
 *
 ****************************************************************************/
static void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	Handle *h = opaque;
	PyObject *eo, *result;
	CallState *cs;

	cs = CallState_get(h);
	if (!h->error_cb) {
		/* No callback defined */
		goto done;
	}

	eo = KafkaError_new0(err, "%s", reason);
	result = PyObject_CallFunctionObjArgs(h->error_cb, eo, NULL);
	Py_DECREF(eo);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(h->rk);
	}

 done:
	CallState_resume(cs);
}

static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
	Handle *h = opaque;
	PyObject *eo = NULL, *result = NULL;
	CallState *cs = NULL;

	cs = CallState_get(h);
	if (json_len == 0) {
		/* No data returned*/
		goto done;
	}

	eo = Py_BuildValue("s", json);
	result = PyObject_CallFunctionObjArgs(h->stats_cb, eo, NULL);
	Py_DECREF(eo);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(h->rk);
	}

 done:
	CallState_resume(cs);
	return 0;
}

static void log_cb (const rd_kafka_t *rk, int level,
                    const char *fac, const char *buf) {
        Handle *h = rd_kafka_opaque(rk);
        PyObject *result;
        CallState *cs;
        static const int level_map[8] = {
                /* Map syslog levels to python logging levels */
                [0] = 50, /* LOG_EMERG   -> logging.CRITICAL */
                [1] = 50, /* LOG_ALERT   -> logging.CRITICAL */
                [2] = 50, /* LOG_CRIT    -> logging.CRITICAL */
                [3] = 40, /* LOG_ERR     -> logging.ERROR */
                [4] = 30, /* LOG_WARNING -> logging.WARNING */
                [5] = 20, /* LOG_NOTICE  -> logging.INFO */
                [6] = 20, /* LOG_INFO    -> logging.INFO */
                [7] = 10, /* LOG_DEBUG   -> logging.DEBUG */
        };

        cs = CallState_get(h);
        result = PyObject_CallMethod(h->logger, "log", "issss",
                                     level_map[level],
                                     "%s [%s] %s",
                                     fac, rd_kafka_name(rk), buf);

        if (result)
                Py_DECREF(result);
        else {
                CallState_crash(cs);
                rd_kafka_yield(h->rk);
        }

        CallState_resume(cs);
}

/****************************************************************************
 *
 *
 * Common helpers
 *
 *
 *
 *
 ****************************************************************************/



/**
 * Clear Python object references in Handle
 */
void Handle_clear (Handle *h) {
	if (h->error_cb)
		Py_DECREF(h->error_cb);

	if (h->stats_cb)
		Py_DECREF(h->stats_cb);

        Py_XDECREF(h->logger);

        if (h->initiated)
                PyThread_delete_key(h->tlskey);
}

/**
 * GC traversal for Python object references
 */
int Handle_traverse (Handle *h, visitproc visit, void *arg) {
	if (h->error_cb)
		Py_VISIT(h->error_cb);

	if (h->stats_cb)
		Py_VISIT(h->stats_cb);

	return 0;
}



/**
 * Populate topic conf from provided dict.
 *
 * Will raise an exception on error and return -1, or returns 0 on success.
 */
static int populate_topic_conf (rd_kafka_topic_conf_t *tconf, const char *what,
				PyObject *dict) {
	Py_ssize_t pos = 0;
	PyObject *ko, *vo;

	if (!PyDict_Check(dict)) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
				 "%s: requires a dict", what);
		return -1;
	}

	while (PyDict_Next(dict, &pos, &ko, &vo)) {
		PyObject *ks, *ks8;
		PyObject *vs, *vs8;
		const char *k;
		const char *v;
		char errstr[256];

		if (!(ks = cfl_PyObject_Unistr(ko))) {
			PyErr_SetString(PyExc_TypeError,
					"expected configuration property "
					"value as type unicode string");
			return -1;
		}

		if (!(vs = cfl_PyObject_Unistr(vo))) {
			PyErr_SetString(PyExc_TypeError,
					"expected configuration property "
					"value as type unicode string");
			Py_DECREF(ks);
			return -1;
		}

		k = cfl_PyUnistr_AsUTF8(ks, &ks8);
		v = cfl_PyUnistr_AsUTF8(vs, &vs8);

		if (rd_kafka_topic_conf_set(tconf, k, v,
					    errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
					 "%s: %s", what, errstr);
                        Py_XDECREF(ks8);
                        Py_XDECREF(vs8);
			Py_DECREF(ks);
			Py_DECREF(vs);
			return -1;
		}

                Py_XDECREF(ks8);
                Py_XDECREF(vs8);
		Py_DECREF(ks);
		Py_DECREF(vs);
	}

	return 0;
}



/**
 * @brief Set single special producer config value.
 *
 * @returns 1 if handled, 0 if unknown, or -1 on failure (exception raised).
 */
static int producer_conf_set_special (Handle *self, rd_kafka_conf_t *conf,
				      rd_kafka_topic_conf_t *tconf,
				      const char *name, PyObject *valobj) {
	PyObject *vs;
	const char *val;

	if (!strcasecmp(name, "on_delivery")) {
		if (!PyCallable_Check(valobj)) {
			cfl_PyErr_Format(
				RD_KAFKA_RESP_ERR__INVALID_ARG,
				"%s requires a callable "
				"object", name);
			return -1;
		}

		self->u.Producer.default_dr_cb = valobj;
		Py_INCREF(self->u.Producer.default_dr_cb);

		return 1;

	} else if (!strcasecmp(name, "partitioner") ||
		   !strcasecmp(name, "partitioner_callback")) {

		if ((vs = cfl_PyObject_Unistr(valobj))) {
			/* Use built-in C partitioners,
			 * based on their name. */
                        PyObject *vs8;
			val = cfl_PyUnistr_AsUTF8(vs, &vs8);

			if (!strcmp(val, "random"))
				rd_kafka_topic_conf_set_partitioner_cb(
					tconf, rd_kafka_msg_partitioner_random);
			else if (!strcmp(val, "consistent"))
				rd_kafka_topic_conf_set_partitioner_cb(
					tconf, rd_kafka_msg_partitioner_consistent);
			else if (!strcmp(val, "consistent_random"))
				rd_kafka_topic_conf_set_partitioner_cb(
					tconf, rd_kafka_msg_partitioner_consistent_random);
			else {
				cfl_PyErr_Format(
					RD_KAFKA_RESP_ERR__INVALID_ARG,
					"unknown builtin partitioner: %s "
					"(available: random, consistent, consistent_random)",
					val);
                                Py_XDECREF(vs8);
				Py_DECREF(vs);
				return -1;
			}

                        Py_XDECREF(vs8);
			Py_DECREF(vs);

		} else {
			/* Custom partitioner (Python callback) */

			if (!PyCallable_Check(valobj)) {
				cfl_PyErr_Format(
					RD_KAFKA_RESP_ERR__INVALID_ARG,
					"%s requires a callable "
					"object", name);
				return -1;
			}

			 /* FIXME: Error out until GIL+rdkafka lock-ordering is fixed. */
			if (1) {
				cfl_PyErr_Format(
					RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
					"custom partitioner support not yet implemented");
				return -1;
			}

			if (self->u.Producer.partitioner_cb)
				Py_DECREF(self->u.Producer.partitioner_cb);

			self->u.Producer.partitioner_cb = valobj;
			Py_INCREF(self->u.Producer.partitioner_cb);

			/* Use trampoline to call Python code. */
			rd_kafka_topic_conf_set_partitioner_cb(tconf,
							       Producer_partitioner_cb);
		}

		return 1;

        } else if (!strcmp(name, "delivery.report.only.error")) {
                /* Since we allocate msgstate for each produced message
                 * with a callback we can't use delivery.report.only.error
                 * as-is, as we wouldn't be able to ever free those msgstates.
                 * Instead we shortcut this setting in the Python client,
                 * providing the same functionality from dr_msg_cb trampoline.
                 */

                if (!PyBool_Check(valobj)) {
                        cfl_PyErr_Format(
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "%s requires bool", name);
                        return -1;
                }

                self->u.Producer.dr_only_error = valobj == Py_True;

                return 1;
        }

	return 0; /* Not handled */
}


/**
 * @brief Set single special consumer config value.
 *
 * @returns 1 if handled, 0 if unknown, or -1 on failure (exception raised).
 */
static int consumer_conf_set_special (Handle *self, rd_kafka_conf_t *conf,
				      rd_kafka_topic_conf_t *tconf,
				      const char *name, PyObject *valobj) {

	if (!strcasecmp(name, "on_commit")) {
		if (!PyCallable_Check(valobj)) {
			cfl_PyErr_Format(
				RD_KAFKA_RESP_ERR__INVALID_ARG,
				"%s requires a callable "
				"object", name);
			return -1;
		}

		self->u.Consumer.on_commit = valobj;
		Py_INCREF(self->u.Consumer.on_commit);

		return 1;
	}

	return 0;
}


/**
 * Common config setup for Kafka client handles.
 *
 * Returns a conf object on success or NULL on failure in which case
 * an exception has been raised.
 */
rd_kafka_conf_t *common_conf_setup (rd_kafka_type_t ktype,
				    Handle *h,
				    PyObject *args,
				    PyObject *kwargs) {
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *tconf;
	Py_ssize_t pos = 0;
	PyObject *ko, *vo;
        PyObject *confdict = NULL;
	int32_t (*partitioner_cb) (const rd_kafka_topic_t *,
				   const void *, size_t, int32_t,
				   void *, void *) = partitioner_cb;

        if (rd_kafka_version() < MIN_RD_KAFKA_VERSION) {
                PyErr_Format(PyExc_RuntimeError,
                             "%s: librdkafka version %s (0x%x) detected",
                             MIN_VER_ERRSTR, rd_kafka_version_str(),
                             rd_kafka_version());
                return NULL;
        }

        /* Supported parameter constellations:
         *  - kwargs (conf={..}, logger=..)
         *  - args and kwargs ({..}, logger=..)
         *  - args ({..})
         * When both args and kwargs are present the kwargs take
         * precedence in case of duplicate keys.
         * All keys map to configuration properties.
         */
        if (args) {
                if (!PyTuple_Check(args) ||
                    PyTuple_Size(args) > 1) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected tuple containing single dict");
                        return NULL;
                } else if (PyTuple_Size(args) == 1 &&
                           !PyDict_Check((confdict = PyTuple_GetItem(args, 0)))) {
                                PyErr_SetString(PyExc_TypeError,
                                                "expected configuration dict");
                                return NULL;
                }
        }

        if (!confdict) {
                if (!kwargs) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected configuration dict");
                        return NULL;
                }

                confdict = kwargs;

        } else if (kwargs) {
                /* Update confdict with kwargs */
                PyDict_Update(confdict, kwargs);
        }

	conf = rd_kafka_conf_new();
	tconf = rd_kafka_topic_conf_new();

        /*
         * Default config (overridable by user)
         */

        /* Enable valid offsets in delivery reports */
        rd_kafka_topic_conf_set(tconf, "produce.offset.report", "true", NULL, 0);

	/* Convert config dict to config key-value pairs. */
	while (PyDict_Next(confdict, &pos, &ko, &vo)) {
		PyObject *ks, *ks8;
		PyObject *vs = NULL, *vs8 = NULL;
		const char *k;
		const char *v;
		char errstr[256];
		int r;

		if (!(ks = cfl_PyObject_Unistr(ko))) {
			PyErr_SetString(PyExc_TypeError,
					"expected configuration property name "
					"as type unicode string");
			rd_kafka_topic_conf_destroy(tconf);
			rd_kafka_conf_destroy(conf);
			return NULL;
		}

		k = cfl_PyUnistr_AsUTF8(ks, &ks8);
		if (!strcmp(k, "default.topic.config")) {
			if (populate_topic_conf(tconf, k, vo) == -1) {
				Py_DECREF(ks);
				rd_kafka_topic_conf_destroy(tconf);
				rd_kafka_conf_destroy(conf);
				return NULL;
			}
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			continue;

		} else if (!strcmp(k, "error_cb")) {
			if (!PyCallable_Check(vo)) {
				PyErr_SetString(PyExc_TypeError,
						"expected error_cb property "
						"as a callable function");
				rd_kafka_topic_conf_destroy(tconf);
				rd_kafka_conf_destroy(conf);
                                Py_XDECREF(ks8);
				Py_DECREF(ks);
				return NULL;
                        }
			if (h->error_cb) {
				Py_DECREF(h->error_cb);
				h->error_cb = NULL;
			}
			if (vo != Py_None) {
				h->error_cb = vo;
				Py_INCREF(h->error_cb);
			}
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			continue;
		} else if (!strcmp(k, "stats_cb")) {
			if (!PyCallable_Check(vo)) {
				PyErr_SetString(PyExc_TypeError,
						"expected stats_cb property "
						"as a callable function");
				rd_kafka_topic_conf_destroy(tconf);
				rd_kafka_conf_destroy(conf);
                                Py_XDECREF(ks8);
				Py_DECREF(ks);
				return NULL;
                        }

			if (h->stats_cb) {
				Py_DECREF(h->stats_cb);
				h->stats_cb = NULL;
			}
			if (vo != Py_None) {
				h->stats_cb = vo;
				Py_INCREF(h->stats_cb);
			}
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			continue;
                } else if (!strcmp(k, "logger")) {
                        if (h->logger) {
                                Py_DECREF(h->logger);
                                h->logger = NULL;
                        }

                        if (vo != Py_None) {
                                h->logger = vo;
                                Py_INCREF(h->logger);
                        }
                        Py_XDECREF(ks8);
                        Py_DECREF(ks);
                        continue;
                }

		/* Special handling for certain config keys. */
		if (ktype == RD_KAFKA_PRODUCER)
			r = producer_conf_set_special(h, conf, tconf, k, vo);
		else
			r = consumer_conf_set_special(h, conf, tconf, k, vo);
		if (r == -1) {
			/* Error */
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			rd_kafka_topic_conf_destroy(tconf);
			rd_kafka_conf_destroy(conf);
			return NULL;

		} else if (r == 1) {
			/* Handled */
			continue;
		}


		/*
		 * Pass configuration property through to librdkafka.
		 */
                if (vo == Py_None) {
                        v = NULL;
                } else {
                        if (!(vs = cfl_PyObject_Unistr(vo))) {
                                PyErr_SetString(PyExc_TypeError,
                                                "expected configuration "
                                                "property value as type "
                                                "unicode string");
                                rd_kafka_topic_conf_destroy(tconf);
                                rd_kafka_conf_destroy(conf);
                                Py_XDECREF(ks8);
                                Py_DECREF(ks);
                                return NULL;
                        }
                        v = cfl_PyUnistr_AsUTF8(vs, &vs8);
                }

		if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
					  "%s", errstr);
			rd_kafka_topic_conf_destroy(tconf);
			rd_kafka_conf_destroy(conf);
                        Py_XDECREF(vs8);
                        Py_XDECREF(vs);
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			return NULL;
		}

                Py_XDECREF(vs8);
                Py_XDECREF(vs);
                Py_XDECREF(ks8);
		Py_DECREF(ks);
	}

	if (h->error_cb)
		rd_kafka_conf_set_error_cb(conf, error_cb);

	if (h->stats_cb)
		rd_kafka_conf_set_stats_cb(conf, stats_cb);

        if (h->logger) {
                /* Write logs to log queue (which is forwarded
                 * to the polled queue in the Producer/Consumer constructors) */
                rd_kafka_conf_set(conf, "log.queue", "true", NULL, 0);
                rd_kafka_conf_set_log_cb(conf, log_cb);
        }

	rd_kafka_topic_conf_set_opaque(tconf, h);
	rd_kafka_conf_set_default_topic_conf(conf, tconf);

	rd_kafka_conf_set_opaque(conf, h);

	h->tlskey = PyThread_create_key();
        h->initiated = 1;

	return conf;
}




/**
 * @brief Initialiase a CallState and unlock the GIL prior to a
 *        possibly blocking external call.
 */
void CallState_begin (Handle *h, CallState *cs) {
	cs->thread_state = PyEval_SaveThread();
	assert(cs->thread_state != NULL);
	cs->crashed = 0;
	PyThread_set_key_value(h->tlskey, cs);
}

/**
 * @brief Relock the GIL after external call is done.
 * @returns 0 if a Python signal was raised or a callback crashed, else 1.
 */
int CallState_end (Handle *h, CallState *cs) {
	PyThread_delete_key_value(h->tlskey);

	PyEval_RestoreThread(cs->thread_state);

	if (PyErr_CheckSignals() == -1 || cs->crashed)
		return 0;

	return 1;
}


/**
 * @brief Get the current thread's CallState and re-locks the GIL.
 */
CallState *CallState_get (Handle *h) {
	CallState *cs = PyThread_get_key_value(h->tlskey);
	assert(cs != NULL);
	assert(cs->thread_state != NULL);
	PyEval_RestoreThread(cs->thread_state);
	cs->thread_state = NULL;
	return cs;
}

/**
 * @brief Un-locks the GIL to resume blocking external call.
 */
void CallState_resume (CallState *cs) {
	assert(cs->thread_state == NULL);
	cs->thread_state = PyEval_SaveThread();
}

/**
 * @brief Indicate that call crashed.
 */
void CallState_crash (CallState *cs) {
	cs->crashed++;
}



/****************************************************************************
 *
 *
 * Base
 *
 *
 *
 *
 ****************************************************************************/


static PyObject *libversion (PyObject *self, PyObject *args) {
	return Py_BuildValue("si",
			     rd_kafka_version_str(),
			     rd_kafka_version());
}

static PyObject *version (PyObject *self, PyObject *args) {
	return Py_BuildValue("si", "0.11.4", 0x000b0400);
}

static PyMethodDef cimpl_methods[] = {
	{"libversion", libversion, METH_NOARGS,
	 "  Retrieve librdkafka version string and integer\n"
	 "\n"
	 "  :returns: (version_string, version_int) tuple\n"
	 "  :rtype: tuple(str,int)\n"
	 "\n"
	},
	{"version", version, METH_NOARGS,
	 "  Retrieve module version string and integer\n"
	 "\n"
	 "  :returns: (version_string, version_int) tuple\n"
	 "  :rtype: tuple(str,int)\n"
	 "\n"
	},
	{ NULL }
};


/**
 * @brief Add librdkafka error enums to KafkaError's type dict.
 * @returns an updated doc string containing all error constants.
 */
static char *KafkaError_add_errs (PyObject *dict, const char *origdoc) {
	const struct rd_kafka_err_desc *descs;
	size_t cnt;
	size_t i;
	char *doc;
	size_t dof = 0, dsize;
	/* RST grid table column widths */
#define _COL1_W 50
#define _COL2_W 100 /* Must be larger than COL1 */
	char dash[_COL2_W], eq[_COL2_W];

	rd_kafka_get_err_descs(&descs, &cnt);

	memset(dash, '-', sizeof(dash));
	memset(eq, '=', sizeof(eq));

	/* Setup output doc buffer. */
	dof = strlen(origdoc);
	dsize = dof + 500 + (cnt * 200);
	doc = malloc(dsize);
	memcpy(doc, origdoc, dof+1);

#define _PRINT(...) do {						\
		char tmpdoc[512];					\
		size_t _len;						\
		_len = snprintf(tmpdoc, sizeof(tmpdoc), __VA_ARGS__);	\
		if (_len > sizeof(tmpdoc)) _len = sizeof(tmpdoc)-1;	\
		if (dof + _len >= dsize) {				\
			dsize += 2;					\
			doc = realloc(doc, dsize);			\
		}							\
		memcpy(doc+dof, tmpdoc, _len+1);			\
		dof += _len;						\
	} while (0)

	/* Error constant table header (RST grid table) */
	_PRINT("Error and event constants:\n\n"
	       "+-%.*s-+-%.*s-+\n"
	       "| %-*.*s | %-*.*s |\n"
	       "+=%.*s=+=%.*s=+\n",
	       _COL1_W, dash, _COL2_W, dash,
	       _COL1_W, _COL1_W, "Constant", _COL2_W, _COL2_W, "Description",
	       _COL1_W, eq, _COL2_W, eq);

	for (i = 0 ; i < cnt ; i++) {
		PyObject *code;

		if (!descs[i].desc)
			continue;

		code = PyLong_FromLong(descs[i].code);

		PyDict_SetItemString(dict, descs[i].name, code);

		Py_DECREF(code);

		_PRINT("| %-*.*s | %-*.*s |\n"
		       "+-%.*s-+-%.*s-+\n",
		       _COL1_W, _COL1_W, descs[i].name,
		       _COL2_W, _COL2_W, descs[i].desc,
		       _COL1_W, dash, _COL2_W, dash);
	}

	_PRINT("\n");

	return doc; // FIXME: leak
}


#ifdef PY3
static struct PyModuleDef cimpl_moduledef = {
	PyModuleDef_HEAD_INIT,
	"cimpl",                                  /* m_name */
	"Confluent's Apache Kafka Python client (C implementation)", /* m_doc */
	-1,                                       /* m_size */
	cimpl_methods,                            /* m_methods */
};
#endif


static PyObject *_init_cimpl (void) {
	PyObject *m;

	if (PyType_Ready(&KafkaErrorType) < 0)
		return NULL;
	if (PyType_Ready(&MessageType) < 0)
		return NULL;
	if (PyType_Ready(&TopicPartitionType) < 0)
		return NULL;
	if (PyType_Ready(&ProducerType) < 0)
		return NULL;
	if (PyType_Ready(&ConsumerType) < 0)
		return NULL;

#ifdef PY3
	m = PyModule_Create(&cimpl_moduledef);
#else
	m = Py_InitModule3("cimpl", cimpl_methods,
			   "Confluent's Apache Kafka Python client (C implementation)");
#endif
	if (!m)
		return NULL;

	Py_INCREF(&KafkaErrorType);
	KafkaErrorType.tp_doc =
		KafkaError_add_errs(KafkaErrorType.tp_dict,
				    KafkaErrorType.tp_doc);
	PyModule_AddObject(m, "KafkaError", (PyObject *)&KafkaErrorType);

	Py_INCREF(&MessageType);
	PyModule_AddObject(m, "Message", (PyObject *)&MessageType);

	Py_INCREF(&TopicPartitionType);
	PyModule_AddObject(m, "TopicPartition",
			   (PyObject *)&TopicPartitionType);

	Py_INCREF(&ProducerType);
	PyModule_AddObject(m, "Producer", (PyObject *)&ProducerType);

	Py_INCREF(&ConsumerType);
	PyModule_AddObject(m, "Consumer", (PyObject *)&ConsumerType);

#if PY_VERSION_HEX >= 0x02070000
	KafkaException = PyErr_NewExceptionWithDoc(
		"cimpl.KafkaException",
		"Kafka exception that wraps the :py:class:`KafkaError` "
		"class.\n"
		"\n"
		"Use ``exception.args[0]`` to extract the "
		":py:class:`KafkaError` object\n"
		"\n",
		NULL, NULL);
#else
        KafkaException = PyErr_NewException("cimpl.KafkaException", NULL, NULL);
#endif
	Py_INCREF(KafkaException);
	PyModule_AddObject(m, "KafkaException", KafkaException);

	PyModule_AddIntConstant(m, "TIMESTAMP_NOT_AVAILABLE", RD_KAFKA_TIMESTAMP_NOT_AVAILABLE);
	PyModule_AddIntConstant(m, "TIMESTAMP_CREATE_TIME", RD_KAFKA_TIMESTAMP_CREATE_TIME);
	PyModule_AddIntConstant(m, "TIMESTAMP_LOG_APPEND_TIME", RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME);

        PyModule_AddIntConstant(m, "OFFSET_BEGINNING", RD_KAFKA_OFFSET_BEGINNING);
        PyModule_AddIntConstant(m, "OFFSET_END", RD_KAFKA_OFFSET_END);
        PyModule_AddIntConstant(m, "OFFSET_STORED", RD_KAFKA_OFFSET_STORED);
        PyModule_AddIntConstant(m, "OFFSET_INVALID", RD_KAFKA_OFFSET_INVALID);

	return m;
}


#ifdef PY3
PyMODINIT_FUNC PyInit_cimpl (void) {
	return _init_cimpl();
}
#else
PyMODINIT_FUNC initcimpl (void) {
	_init_cimpl();
}
#endif
