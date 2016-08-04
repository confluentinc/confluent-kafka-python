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
 *  - TopicPartition.offset should probably be None for the INVALID offset
 *    rather than exposing the special value -1001.
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
		code2 = PyLong_AsLong(o2);

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
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT,        /*tp_flags*/
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
	return KafkaError_new0(err, "%s", str);
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
		return PyLong_FromLong(self->offset);
	else
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
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
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
PyObject *Message_new0 (const rd_kafka_message_t *rkm) {
	Message *self;

	self = (Message *)MessageType.tp_alloc(&MessageType, 0);
	if (!self)
		return NULL;

	self->error = KafkaError_new_or_None(rkm->err,
					     rkm->err ?
					     rd_kafka_message_errstr(rkm) :
					     NULL);

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
typedef struct {
	PyObject_HEAD
	char *topic;
	int   partition;
	int64_t offset;
	PyObject *error;
} TopicPartition;


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

static void TopicPartition_dealloc (TopicPartition *self) {
	PyObject_GC_UnTrack(self);

	TopicPartition_clear(self);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *TopicPartition_new0 (const char *topic, int partition,
				      long long offset,
				      rd_kafka_resp_err_t err);


static PyObject *TopicPartition_new (PyTypeObject *type, PyObject *args,
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
		return NULL;

	return TopicPartition_new0(topic, partition, offset, 0);
}
	


static int TopicPartition_traverse (TopicPartition *self,
				    visitproc visit, void *arg) {
	if (self->error)
		Py_VISIT(self->error);
	return 0;
}


static PyMemberDef TopicPartition_members[] = {
	{ "topic", T_STRING, offsetof(TopicPartition, topic), READONLY,
	  "Topic name" },
	{ "partition", T_INT, offsetof(TopicPartition, partition), 0,
	  "Partition number" },
	{ "offset", T_LONGLONG, offsetof(TopicPartition, offset), 0,
	  "Offset" }, /* FIXME: Possibly use None for INVALID offset (-1001) */
	{ "error", T_OBJECT, offsetof(TopicPartition, error), READONLY,
	  "Indicates an error (with :py:class:`KafkaError`) unless None." },
	{ NULL }
};


static PyObject *TopicPartition_str0 (TopicPartition *self) {
	PyObject *errstr = self->error == Py_None ? NULL :
		cfl_PyObject_Unistr(self->error);
	PyObject *ret;
	ret = cfl_PyUnistr(
		_FromFormat("TopicPartition{topic=%s,partition=%"PRId32
			    ",offset=%"PRId64",error=%s}",
			    self->topic, self->partition,
			    self->offset,
			    errstr ? cfl_PyUnistr_AsUTF8(errstr) : "None"));
	if (errstr)
		Py_DECREF(errstr);
	return ret;
}


static PyTypeObject TopicPartitionType;

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


static PyTypeObject TopicPartitionType = {
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
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
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
	0,                         /* tp_init */
	0,                         /* tp_alloc */
	TopicPartition_new     /* tp_new */
};

/**
 * @brief Internal factory to create a TopicPartition object.
 */
static PyObject *TopicPartition_new0 (const char *topic, int partition,
				      long long offset,
				      rd_kafka_resp_err_t err) {
	TopicPartition *self;

	self = (TopicPartition *)TopicPartitionType.tp_alloc(
		&TopicPartitionType, 0);
	if (!self)
		return NULL;

	self->topic = strdup(topic);
	self->partition = partition;
	self->offset = offset;
	self->error = KafkaError_new_or_None(err, NULL);

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

	c_parts = rd_kafka_topic_partition_list_new(PyList_Size(plist));

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
		PyObject *ks;
		PyObject *vs;
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

		k = cfl_PyUnistr_AsUTF8(ks);
		v = cfl_PyUnistr_AsUTF8(vs);

		if (rd_kafka_topic_conf_set(tconf, k, v,
					    errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
					 "%s: %s", what, errstr);
			Py_DECREF(ks);
			Py_DECREF(vs);
			return -1;
		}

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
static int producer_conf_set_special (Producer *self, rd_kafka_conf_t *conf,
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

		self->default_dr_cb = valobj;
		Py_INCREF(self->default_dr_cb);

		return 1;

	} else if (!strcasecmp(name, "partitioner") ||
		   !strcasecmp(name, "partitioner_callback")) {

		if ((vs = cfl_PyObject_Unistr(valobj))) {
			/* Use built-in C partitioners,
			 * based on their name. */
			val = cfl_PyUnistr_AsUTF8(vs);

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
				Py_DECREF(vs);
				return -1;
			}

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

			if (self->partitioner_cb)
				Py_DECREF(self->partitioner_cb);

			self->partitioner_cb = valobj;
			Py_INCREF(self->partitioner_cb);

			/* Use trampoline to call Python code. */
			rd_kafka_topic_conf_set_partitioner_cb(tconf,
							       Producer_partitioner_cb);
		}

		return 1;
	}

	return 0; /* Not handled */
}


/**
 * @brief Set single special consumer config value.
 *
 * @returns 1 if handled, 0 if unknown, or -1 on failure (exception raised).
 */
static int consumer_conf_set_special (Consumer *self, rd_kafka_conf_t *conf,
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

		self->on_commit = valobj;
		Py_INCREF(self->on_commit);

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
				    void *self0,
				    PyObject *args,
				    PyObject *kwargs) {
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *tconf;
	Py_ssize_t pos = 0;
	PyObject *ko, *vo;
	int32_t (*partitioner_cb) (const rd_kafka_topic_t *,
				   const void *, size_t, int32_t,
				   void *, void *) = partitioner_cb;

	if (!kwargs) {
		/* If no kwargs, fall back on single dict arg, if any. */
		if (!args || !PyTuple_Check(args) || PyTuple_Size(args) < 1 ||
		    !PyDict_Check((kwargs = PyTuple_GetItem(args, 0)))) {
			PyErr_SetString(PyExc_TypeError,
					"expected configuration dict");
			return NULL;
		}
	}

	conf = rd_kafka_conf_new();
	tconf = rd_kafka_topic_conf_new();

	/* Convert kwargs dict to config key-value pairs. */
	while (PyDict_Next(kwargs, &pos, &ko, &vo)) {
		PyObject *ks;
		PyObject *vs = NULL;
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

		k = cfl_PyUnistr_AsUTF8(ks);
		if (!strcmp(k, "default.topic.config")) {
			if (populate_topic_conf(tconf, k, vo) == -1) {
				Py_DECREF(ks);
				rd_kafka_topic_conf_destroy(tconf);
				rd_kafka_conf_destroy(conf);
				return NULL;
			}

			Py_DECREF(ks);
			continue;
		}

		/* Special handling for certain config keys. */
		if (ktype == RD_KAFKA_PRODUCER)
			r = producer_conf_set_special((Producer *)self0,
						      conf, tconf, k, vo);
		else
			r = consumer_conf_set_special((Consumer *)self0,
						      conf, tconf, k, vo);
		if (r == -1) {
			/* Error */
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
		if (!(vs = cfl_PyObject_Unistr(vo))) {
			PyErr_SetString(PyExc_TypeError,
					"expected configuration property "
					"value as type unicode string");
			rd_kafka_topic_conf_destroy(tconf);
			rd_kafka_conf_destroy(conf);
			Py_DECREF(ks);
			return NULL;
		}
		v = cfl_PyUnistr_AsUTF8(vs);

		if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
					  "%s", errstr);
			rd_kafka_topic_conf_destroy(tconf);
			rd_kafka_conf_destroy(conf);
			Py_DECREF(vs);
			Py_DECREF(ks);
			return NULL;
		}

		Py_DECREF(vs);
		Py_DECREF(ks);
	}

	rd_kafka_topic_conf_set_opaque(tconf, self0);
	rd_kafka_conf_set_default_topic_conf(conf, tconf);

	rd_kafka_conf_set_opaque(conf, self0);

	return conf;
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
	return Py_BuildValue("si", "0.9.1", 0x00090100);
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

	KafkaException = PyErr_NewExceptionWithDoc(
		"cimpl.KafkaException",
		"Kafka exception that wraps the :py:class:`KafkaError` "
		"class.\n"
		"\n"
		"Use ``exception.args[0]`` to extract the "
		":py:class:`KafkaError` object\n"
		"\n",
		NULL, NULL);
	Py_INCREF(KafkaException);
	PyModule_AddObject(m, "KafkaException", KafkaException);

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
