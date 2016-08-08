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

#include <Python.h>
#include <structmember.h>
#include <pythread.h>

#include <librdkafka/rdkafka.h>

#if PY_MAJOR_VERSION >= 3
#define PY3
#include <bytesobject.h>
#endif



/****************************************************************************
 *
 *
 * Python 2 & 3 portability
 *
 * Binary data (we call it cfl_PyBin):
 *   Python 2: string
 *   Python 3: bytes
 *
 * Unicode Strings (we call it cfl_PyUnistr):
 *   Python 2: unicode
 *   Python 3: strings
 *
 ****************************************************************************/

#ifdef PY3 /* Python 3 */
/**
 * @brief Binary type, use as cfl_PyBin(_X(A,B)) where _X() is the type-less
 *        suffix of a PyBytes/Str_X() function
*/
#define cfl_PyBin(X)    PyBytes ## X

/**
 * @brief Unicode type, same usage as PyBin()
 */
#define cfl_PyUnistr(X) PyUnicode ## X

/**
 * @returns Unicode Python object as char * in UTF-8 encoding
 */
#define cfl_PyUnistr_AsUTF8(X)  PyUnicode_AsUTF8(X)

/**
 * @returns Unicode Python string object
 */
#define cfl_PyObject_Unistr(X)  PyObject_Str(X)

#else /* Python 2 */

/* See comments above */
#define cfl_PyBin(X)    PyString ## X
#define cfl_PyUnistr(X) PyUnicode ## X
#define cfl_PyUnistr_AsUTF8(X) PyBytes_AsString(PyUnicode_AsUTF8String(X))
#define cfl_PyObject_Unistr(X) PyObject_Unicode(X)
#endif


/****************************************************************************
 *
 *
 * KafkaError
 *
 *
 *
 *
 ****************************************************************************/
extern PyObject *KafkaException;

PyObject *KafkaError_new0 (rd_kafka_resp_err_t err, const char *fmt, ...);
PyObject *KafkaError_new_or_None (rd_kafka_resp_err_t err, const char *str);


/**
 * @brief Raise an exception using KafkaError.
 * \p err and and \p ... (string representation of error) is set on the returned
 * KafkaError object.
 */
#define cfl_PyErr_Format(err,...) do {					\
		PyObject *_eo = KafkaError_new0(err, __VA_ARGS__);	\
		PyErr_SetObject(KafkaException, _eo);			\
	} while (0)



/****************************************************************************
 *
 *
 * Common instance handle for both Producer and Consumer
 *
 *
 *
 *
 ****************************************************************************/
typedef struct {
	PyObject_HEAD
	rd_kafka_t *rk;
	PyObject *error_cb;
	int tlskey;  /* Thread-Local-Storage key */

	union {
		/**
		 * Producer
		 */
		struct {
			PyObject *default_dr_cb;
			PyObject *partitioner_cb; /**< Registered Python partitioner */
			int32_t (*c_partitioner_cb) (
				const rd_kafka_topic_t *,
				const void *, size_t, int32_t,
				void *, void *);  /**< Fallback C partitioner*/
		} Producer;

		/**
		 * Consumer
		 */
		struct {
			int rebalance_assigned;  /* Rebalance: Callback performed assign() call.*/
			PyObject *on_assign;     /* Rebalance: on_assign callback */
			PyObject *on_revoke;     /* Rebalance: on_revoke callback */
			PyObject *on_commit;     /* Commit callback */

		} Consumer;
	} u;
} Handle;


void Handle_clear (Handle *h);
int  Handle_traverse (Handle *h, visitproc visit, void *arg);


/**
 * @brief Current thread's state for "blocking" calls to librdkafka.
 */
typedef struct {
	PyThreadState *thread_state;
	int crashed;   /* Callback crashed */
} CallState;

/**
 * @brief Initialiase a CallState and unlock the GIL prior to a
 *        possibly blocking external call.
 */
void CallState_begin (Handle *h, CallState *cs);
/**
 * @brief Relock the GIL after external call is done, remove TLS state.
 * @returns 0 if a Python signal was raised or a callback crashed, else 1.
 */
int CallState_end (Handle *h, CallState *cs);

/**
 * @brief Get the current thread's CallState and re-locks the GIL.
 */
CallState *CallState_get (Handle *h);
/**
 * @brief Un-locks the GIL to resume blocking external call.
 */
void CallState_resume (CallState *cs);

/**
 * @brief Indicate that call crashed.
 */
void CallState_crash (CallState *cs);


/****************************************************************************
 *
 *
 * Common
 *
 *
 *
 *
 ****************************************************************************/
rd_kafka_conf_t *common_conf_setup (rd_kafka_type_t ktype,
				    Handle *h,
				    PyObject *args,
				    PyObject *kwargs);
PyObject *c_parts_to_py (const rd_kafka_topic_partition_list_t *c_parts);
rd_kafka_topic_partition_list_t *py_to_c_parts (PyObject *plist);


/****************************************************************************
 *
 *
 * Message
 *
 *
 *
 *
 ****************************************************************************/

/**
 * @brief confluent_kafka.Message object
 */
typedef struct {
	PyObject_HEAD
	PyObject *topic;
	PyObject *value;
	PyObject *key;
	PyObject *error;
	int32_t partition;
	int64_t offset;
} Message;

extern PyTypeObject MessageType;

PyObject *Message_new0 (const rd_kafka_message_t *rkm);
PyObject *Message_error (Message *self, PyObject *ignore);


/****************************************************************************
 *
 *
 * Producer
 *
 *
 *
 *
 ****************************************************************************/

extern PyTypeObject ProducerType;


int32_t Producer_partitioner_cb (const rd_kafka_topic_t *rkt,
				 const void *keydata,
				 size_t keylen,
				 int32_t partition_cnt,
				 void *rkt_opaque, void *msg_opaque);


/****************************************************************************
 *
 *
 * Consumer
 *
 *
 *
 *
 ****************************************************************************/

extern PyTypeObject ConsumerType;
