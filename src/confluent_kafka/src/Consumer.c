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

/**
 * ⚠️  WARNING: UPDATE TYPE STUBS WHEN MODIFYING INTERFACES ⚠️
 *
 * This file defines the Consumer class and its methods.
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
 * Consumer
 *
 *
 *
 *
 ****************************************************************************/

/**
 * @brief Reject-on-contention gate: only one caller may be inside gated
 *        Consumer C code at a time. `identity` identifies the caller --
 *        for the sync Consumer this is always the calling thread's own ID.
 *        For AIOConsumer this is a temporary ID generated when the method
 *        is called.
 *
 *        If the gate is unowned, `identity` becomes the owner. If
 *        `identity` already matches the current owner, this is a
 *        legitimate re-entrant call (gate_depth is incremented). Any other
 *        identity is rejected with ConcurrentModificationException.
 *
 * @returns 1 if the gate is now held or 0 with ConcurrentModificationException
 *          set if a different identity currently holds it.
 *
 * @warning Only compiled in when CFL_CONSUMER_GATE_ENABLED is true
 *          to preserve backward compatibility. When false, Handle_gate_enter()
 *          always succeeds.
 */
static int Handle_gate_enter(Handle *h, atomic_ulong_t identity) {
#if !CFL_CONSUMER_GATE_ENABLED
        (void)h;
        (void)identity;
        return 1;
#else
        assert(identity != 0);

        if (atomic_ulong_cas(&h->u.Consumer.gate_owner, 0, identity)) {
                /* Gate was unowned: we now own it. */
                h->u.Consumer.gate_depth = 1;
                return 1;
        }

        if (atomic_ulong_get(&h->u.Consumer.gate_owner) == identity) {
                /* Re-entrant call presenting the same identity that already
                 * owns the gate.
                 */
                h->u.Consumer.gate_depth++;
                return 1;
        }

        PyErr_SetString(ConcurrentModificationException,
                        "Illegal concurrent access to this Consumer "
                        "instance from another caller");
        return 0;
#endif
}

/**
 * @brief Counterpart to Handle_gate_enter(): call once per successful
 *        Handle_gate_enter(), on every return path.
 *
 * @warning Only compiled in when CFL_CONSUMER_GATE_ENABLED is true
 *          to preserve backward compatibility. When false, Handle_gate_exit()
 *          is a no-op.
 */
static void Handle_gate_exit(Handle *h) {
#if !CFL_CONSUMER_GATE_ENABLED
        (void)h;
#else
        h->u.Consumer.gate_depth--;
        if (h->u.Consumer.gate_depth == 0)
                atomic_ulong_set(&h->u.Consumer.gate_owner, 0);
#endif
}

static void Consumer_clear0(Handle *self) {
        if (self->u.Consumer.on_assign) {
                Py_DECREF(self->u.Consumer.on_assign);
                self->u.Consumer.on_assign = NULL;
        }
        if (self->u.Consumer.on_revoke) {
                Py_DECREF(self->u.Consumer.on_revoke);
                self->u.Consumer.on_revoke = NULL;
        }
        if (self->u.Consumer.on_lost) {
                Py_DECREF(self->u.Consumer.on_lost);
                self->u.Consumer.on_lost = NULL;
        }
        if (self->u.Consumer.on_commit) {
                Py_DECREF(self->u.Consumer.on_commit);
                self->u.Consumer.on_commit = NULL;
        }
        if (self->u.Consumer.rkqu) {
                rd_kafka_queue_destroy(self->u.Consumer.rkqu);
                self->u.Consumer.rkqu = NULL;
        }
}

static int Consumer_clear(Handle *self) {
        Consumer_clear0(self);
        Handle_clear(self);
        return 0;
}

static void Consumer_dealloc(Handle *self) {
        PyObject_GC_UnTrack(self);

        Consumer_clear0(self);

        if (self->rk) {
                CallState cs;

                CallState_begin(self, &cs);

                rd_kafka_destroy_flags(self->rk,
                                       RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

                CallState_end(self, &cs);
        }

        Handle_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Consumer_traverse(Handle *self, visitproc visit, void *arg) {
        if (self->u.Consumer.on_assign)
                Py_VISIT(self->u.Consumer.on_assign);
        if (self->u.Consumer.on_revoke)
                Py_VISIT(self->u.Consumer.on_revoke);
        if (self->u.Consumer.on_lost)
                Py_VISIT(self->u.Consumer.on_lost);
        if (self->u.Consumer.on_commit)
                Py_VISIT(self->u.Consumer.on_commit);

        Handle_traverse(self, visit, arg);

        return 0;
}


static PyObject *
Consumer__subscribe_internal(Handle *self, PyObject *args, PyObject *kwargs) {

        rd_kafka_topic_partition_list_t *topics;
        static char *kws[] = {"identity", "topics", "on_assign", "on_revoke",
                              "on_lost", NULL};
        PyObject *tlist, *on_assign = NULL, *on_revoke = NULL, *on_lost = NULL;
        PyObject *result = NULL;
        Py_ssize_t pos = 0;
        rd_kafka_resp_err_t err;
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO|OOO", kws,
                                         &identity, &tlist, &on_assign,
                                         &on_revoke, &on_lost))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!PyList_Check(tlist)) {
                PyErr_Format(PyExc_TypeError,
                             "expected list of topic unicode strings");
                goto done;
        }

        if (on_assign && !PyCallable_Check(on_assign)) {
                PyErr_Format(PyExc_TypeError, "on_assign expects a callable");
                goto done;
        }

        if (on_revoke && !PyCallable_Check(on_revoke)) {
                PyErr_Format(PyExc_TypeError, "on_revoke expects a callable");
                goto done;
        }

        if (on_lost && !PyCallable_Check(on_lost)) {
                PyErr_Format(PyExc_TypeError, "on_lost expects a callable");
                goto done;
        }

        topics = rd_kafka_topic_partition_list_new((int)PyList_Size(tlist));
        for (pos = 0; pos < PyList_Size(tlist); pos++) {
                PyObject *o = PyList_GetItem(tlist, pos);
                PyObject *uo, *uo8;
                if (!(uo = cfl_PyObject_Unistr(o))) {
                        PyErr_Format(PyExc_TypeError,
                                     "expected list of unicode strings");
                        rd_kafka_topic_partition_list_destroy(topics);
                        goto done;
                }
                rd_kafka_topic_partition_list_add(topics,
                                                  cfl_PyUnistr_AsUTF8(uo, &uo8),
                                                  RD_KAFKA_PARTITION_UA);
                Py_XDECREF(uo8);
                Py_DECREF(uo);
        }

        err = rd_kafka_subscribe(self->rk, topics);

        rd_kafka_topic_partition_list_destroy(topics);

        if (err) {
                cfl_PyErr_Format(err, "Failed to set subscription: %s",
                                 rd_kafka_err2str(err));
                goto done;
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

        if (self->u.Consumer.on_lost) {
                Py_DECREF(self->u.Consumer.on_lost);
                self->u.Consumer.on_lost = NULL;
        }
        if (on_lost) {
                self->u.Consumer.on_lost = on_lost;
                Py_INCREF(self->u.Consumer.on_lost);
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

static PyObject *Consumer__unsubscribe_internal(Handle *self,
                                                PyObject *args) {
        atomic_ulong_t identity;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;

        if (!PyArg_ParseTuple(args, "k", &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        err = rd_kafka_unsubscribe(self->rk);
        if (err) {
                cfl_PyErr_Format(err, "Failed to remove subscription: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}


static PyObject *
Consumer__incremental_assign_internal(Handle *self, PyObject *args) {
        atomic_ulong_t identity;
        PyObject *tlist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_error_t *error;

        if (!PyArg_ParseTuple(args, "kO", &identity, &tlist))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(tlist)))
                goto done;

        self->u.Consumer.rebalance_incremental_assigned++;

        error = rd_kafka_incremental_assign(self->rk, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry points for assign().
 */
static PyObject *Consumer__assign_internal(Handle *self, PyObject *args) {
        atomic_ulong_t identity;
        PyObject *tlist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;

        if (!PyArg_ParseTuple(args, "kO", &identity, &tlist))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(tlist)))
                goto done;

        self->u.Consumer.rebalance_assigned++;

        err = rd_kafka_assign(self->rk, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);

        if (err) {
                cfl_PyErr_Format(err, "Failed to set assignment: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry points for unassign().
 */
static PyObject *Consumer__unassign_internal(Handle *self, PyObject *args) {
        atomic_ulong_t identity;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;

        if (!PyArg_ParseTuple(args, "k", &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        self->u.Consumer.rebalance_assigned++;

        err = rd_kafka_assign(self->rk, NULL);
        if (err) {
                cfl_PyErr_Format(err, "Failed to remove assignment: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry points for incremental unassign().
 */
static PyObject *
Consumer__incremental_unassign_internal(Handle *self, PyObject *args) {
        atomic_ulong_t identity;
        PyObject *tlist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_error_t *error;

        if (!PyArg_ParseTuple(args, "kO", &identity, &tlist))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(tlist)))
                goto done;

        self->u.Consumer.rebalance_incremental_unassigned++;

        error = rd_kafka_incremental_unassign(self->rk, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry point for assignment().
 */
static PyObject *
Consumer__assignment_internal(Handle *self, PyObject *args,
                              PyObject *kwargs) {

        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        atomic_ulong_t identity;
        static char *kws[] = {"identity", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "k", kws, &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        err = rd_kafka_assignment(self->rk, &c_parts);
        if (err) {
                cfl_PyErr_Format(err, "Failed to get assignment: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }


        result = c_parts_to_py(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Global offset commit on_commit callback trampoline triggered
 *        from poll() et.al
 */
static void Consumer_offset_commit_cb(rd_kafka_t *rk,
                                      rd_kafka_resp_err_t err,
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
                CallState_fetch_exception(cs);
                CallState_crash(cs);
                rd_kafka_yield(rk);
        }

        CallState_resume(cs);
}

/**
 * @brief Simple struct to pass results from commit from offset_commit_return_cb
 *        back to offset_commit() return value.
 */
struct commit_return {
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *c_parts;
};

/**
 * @brief Simple offset_commit_cb to pass the callback information
 *        as return value from commit() through the commit_return struct.
 *        Triggered from rd_kafka_commit_queue().
 */
static void
Consumer_offset_commit_return_cb(rd_kafka_t *rk,
                                 rd_kafka_resp_err_t err,
                                 rd_kafka_topic_partition_list_t *c_parts,
                                 void *opaque) {
        struct commit_return *commit_return = opaque;

        commit_return->err = err;
        if (c_parts)
                commit_return->c_parts =
                    rd_kafka_topic_partition_list_copy(c_parts);
}


/**
 * @brief Identity-carrying entry point for commit().
 */
static PyObject *Consumer__commit_internal(Handle *self, PyObject *args,
                                           PyObject *kwargs) {
        atomic_ulong_t identity;
        Py_ssize_t nargs;
        PyObject *inner_args;
        PyObject *identity_obj;
        rd_kafka_resp_err_t err;
        PyObject *msg = NULL, *offsets = NULL, *async_o = NULL;
        rd_kafka_topic_partition_list_t *c_offsets;
        int async              = 1;
        static char *kws[]     = {"message", "offsets", "async", "asynchronous",
                                  NULL};
        rd_kafka_queue_t *rkqu = NULL;
        struct commit_return commit_return;
        PyThreadState *thread_state;

        nargs = PyTuple_GET_SIZE(args);
        if (nargs < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "_commit_internal requires a leading "
                                "identity argument");
                return NULL;
        }

        identity_obj = PyTuple_GET_ITEM(args, 0);
        identity     = (atomic_ulong_t)PyLong_AsUnsignedLong(identity_obj);
        if (identity == (atomic_ulong_t)-1 && PyErr_Occurred())
                return NULL;

        inner_args = PyTuple_GetSlice(args, 1, nargs);
        if (!inner_args)
                return NULL;

        if (!Handle_gate_enter(self, identity)) {
                Py_DECREF(inner_args);
                return NULL;
        }

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                Handle_gate_exit(self);
                Py_DECREF(inner_args);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(inner_args, kwargs, "|OOOO", kws,
                                         &msg, &offsets, &async_o,
                                         &async_o)) {
                Handle_gate_exit(self);
                Py_DECREF(inner_args);
                return NULL;
        }

        Py_DECREF(inner_args);

        msg     = msg == Py_None ? NULL : msg;
        offsets = offsets == Py_None ? NULL : offsets;

        if (msg && offsets) {
                PyErr_SetString(PyExc_ValueError,
                                "message and offsets are mutually exclusive");
                Handle_gate_exit(self);
                return NULL;
        }

        if (async_o)
                async = PyObject_IsTrue(async_o);


        if (offsets) {

                if (!(c_offsets = py_to_c_parts(offsets))) {
                        Handle_gate_exit(self);
                        return NULL;
                }
        } else if (msg) {
                Message *m;
                PyObject *uo8;
                rd_kafka_topic_partition_t *rktpar;

                if (PyObject_Type((PyObject *)msg) !=
                    (PyObject *)&MessageType) {
                        PyErr_Format(PyExc_TypeError, "expected %s",
                                     MessageType.tp_name);
                        Handle_gate_exit(self);
                        return NULL;
                }

                m = (Message *)msg;

                if (m->error && m->error != Py_None) {
                        PyObject *error = Message_error(m, NULL);
                        PyObject *errstr =
                            PyObject_CallMethod(error, "str", NULL);
                        cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
                                         "Cannot commit offsets for message "
                                         "with error: '%s'",
                                         PyUnicode_AsUTF8(errstr));
                        Py_DECREF(error);
                        Py_DECREF(errstr);
                        Handle_gate_exit(self);
                        return NULL;
                }

                c_offsets = rd_kafka_topic_partition_list_new(1);
                rktpar    = rd_kafka_topic_partition_list_add(
                    c_offsets, cfl_PyUnistr_AsUTF8(m->topic, &uo8),
                    m->partition);
                rktpar->offset = m->offset + 1;
                rd_kafka_topic_partition_set_leader_epoch(rktpar,
                                                          m->leader_epoch);
                Py_XDECREF(uo8);

        } else {
                c_offsets = NULL;
        }

        if (async) {
                /* Async mode: Use consumer queue for offset commit
                 *             served by consumer_poll() */
                rkqu = self->u.Consumer.rkqu;

        } else {
                /* Sync mode: Let commit_queue() trigger the callback. */
                memset(&commit_return, 0, sizeof(commit_return));

                /* Unlock GIL while we are blocking. */
                thread_state = PyEval_SaveThread();
        }

        err = rd_kafka_commit_queue(self->rk, c_offsets, rkqu,
                                    async ? Consumer_offset_commit_cb
                                          : Consumer_offset_commit_return_cb,
                                    async ? (void *)self
                                          : (void *)&commit_return);

        if (c_offsets)
                rd_kafka_topic_partition_list_destroy(c_offsets);

        if (!async) {
                /* Re-lock GIL */
                PyEval_RestoreThread(thread_state);

                /* Honour inner error (richer) from offset_commit_return_cb */
                if (commit_return.err)
                        err = commit_return.err;
        }

        if (err) {
                /* Outer error from commit_queue() */
                if (!async && commit_return.c_parts)
                        rd_kafka_topic_partition_list_destroy(
                            commit_return.c_parts);

                cfl_PyErr_Format(err, "Commit failed: %s",
                                 rd_kafka_err2str(err));
                Handle_gate_exit(self);
                return NULL;
        }

        if (async) {
                /* async commit returns None when commit is in progress */
                Handle_gate_exit(self);
                Py_RETURN_NONE;

        } else {
                PyObject *plist;

                /* sync commit returns the topic,partition,offset,err list */
                assert(commit_return.c_parts);

                plist = c_parts_to_py(commit_return.c_parts);
                rd_kafka_topic_partition_list_destroy(commit_return.c_parts);

                Handle_gate_exit(self);
                return plist;
        }
}


/**
 * @brief Identity-carrying entry point for store_offsets().
 */
static PyObject *
Consumer__store_offsets_internal(Handle *self, PyObject *args,
                                 PyObject *kwargs) {
#if RD_KAFKA_VERSION < 0x000b0000
        PyErr_Format(PyExc_NotImplementedError,
                     "Consumer store_offsets require "
                     "confluent-kafka-python built for librdkafka "
                     "version >=v0.11.0 (librdkafka runtime 0x%x, "
                     "buildtime 0x%x)",
                     rd_kafka_version(), RD_KAFKA_VERSION);
        return NULL;
#else
        rd_kafka_resp_err_t err;
        PyObject *msg = NULL, *offsets = NULL;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_offsets;
        static char *kws[] = {"identity", "message", "offsets", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "k|OO", kws, &identity,
                                         &msg, &offsets)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        msg     = msg == Py_None ? NULL : msg;
        offsets = offsets == Py_None ? NULL : offsets;

        if (msg && offsets) {
                PyErr_SetString(PyExc_ValueError,
                                "message and offsets are mutually exclusive");
                goto done;
        }

        if (!msg && !offsets) {
                PyErr_SetString(PyExc_ValueError,
                                "expected either message or offsets");
                goto done;
        }

        if (offsets) {

                if (!(c_offsets = py_to_c_parts(offsets)))
                        goto done;
        } else {
                Message *m;
                PyObject *uo8;
                rd_kafka_topic_partition_t *rktpar;

                if (PyObject_Type((PyObject *)msg) !=
                    (PyObject *)&MessageType) {
                        PyErr_Format(PyExc_TypeError, "expected %s",
                                     MessageType.tp_name);
                        goto done;
                }

                m = (Message *)msg;

                if (m->error && m->error != Py_None) {
                        PyObject *error = Message_error(m, NULL);
                        PyObject *errstr =
                            PyObject_CallMethod(error, "str", NULL);
                        cfl_PyErr_Format(
                            RD_KAFKA_RESP_ERR__INVALID_ARG,
                            "Cannot store offsets for message with error: '%s'",
                            PyUnicode_AsUTF8(errstr));
                        Py_DECREF(error);
                        Py_DECREF(errstr);
                        goto done;
                }

                c_offsets = rd_kafka_topic_partition_list_new(1);
                rktpar    = rd_kafka_topic_partition_list_add(
                    c_offsets, cfl_PyUnistr_AsUTF8(m->topic, &uo8),
                    m->partition);
                rktpar->offset = m->offset + 1;
                rd_kafka_topic_partition_set_leader_epoch(rktpar,
                                                          m->leader_epoch);
                Py_XDECREF(uo8);
        }


        err = rd_kafka_offsets_store(self->rk, c_offsets);
        rd_kafka_topic_partition_list_destroy(c_offsets);



        if (err) {
                cfl_PyErr_Format(err, "StoreOffsets failed: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
#endif
}



/**
 * @brief Identity-carrying entry point for committed().
 */
static PyObject *
Consumer__committed_internal(Handle *self, PyObject *args,
                             PyObject *kwargs) {

        PyObject *plist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        double tmout       = -1.0f;
        static char *kws[] = {"identity", "partitions", "timeout", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO|d", kws, &identity,
                                         &plist, &tmout)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }


        if (!(c_parts = py_to_c_parts(plist)))
                goto done;

        Py_BEGIN_ALLOW_THREADS;
        err = rd_kafka_committed(self->rk, c_parts, cfl_timeout_ms(tmout));
        Py_END_ALLOW_THREADS;

        if (err) {
                rd_kafka_topic_partition_list_destroy(c_parts);
                cfl_PyErr_Format(err, "Failed to get committed offsets: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }


        result = c_parts_to_py(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Identity-carrying entry point for position().
 */
static PyObject *
Consumer__position_internal(Handle *self, PyObject *args,
                            PyObject *kwargs) {

        PyObject *plist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"identity", "partitions", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO", kws, &identity,
                                         &plist)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }


        if (!(c_parts = py_to_c_parts(plist)))
                goto done;

        err = rd_kafka_position(self->rk, c_parts);

        if (err) {
                rd_kafka_topic_partition_list_destroy(c_parts);
                cfl_PyErr_Format(err, "Failed to get position: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }


        result = c_parts_to_py(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry point for pause().
 */
static PyObject *
Consumer__pause_internal(Handle *self, PyObject *args, PyObject *kwargs) {

        PyObject *plist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"identity", "partitions", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO", kws, &identity,
                                         &plist)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(plist)))
                goto done;

        err = rd_kafka_pause_partitions(self->rk, c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);
        if (err) {
                cfl_PyErr_Format(err, "Failed to pause partitions: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry point for resume().
 */
static PyObject *
Consumer__resume_internal(Handle *self, PyObject *args, PyObject *kwargs) {

        PyObject *plist;
        PyObject *result = NULL;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"identity", "partitions", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO", kws, &identity,
                                         &plist)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(plist)))
                goto done;

        err = rd_kafka_resume_partitions(self->rk, c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);
        if (err) {
                cfl_PyErr_Format(err, "Failed to resume partitions: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Identity-carrying entry point for seek().
 */
static PyObject *Consumer__seek_internal(Handle *self, PyObject *args,
                                         PyObject *kwargs) {

        TopicPartition *tp;
        PyObject *result        = NULL;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        static char *kws[]      = {"identity", "partition", NULL};
        rd_kafka_topic_partition_list_t *seek_partitions;
        rd_kafka_topic_partition_t *rktpar;
        rd_kafka_error_t *error;
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO", kws, &identity,
                                         (PyObject **)&tp)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }


        if (PyObject_Type((PyObject *)tp) != (PyObject *)&TopicPartitionType) {
                PyErr_Format(PyExc_TypeError, "expected %s",
                             TopicPartitionType.tp_name);
                goto done;
        }

        seek_partitions = rd_kafka_topic_partition_list_new(1);
        rktpar = rd_kafka_topic_partition_list_add(seek_partitions, tp->topic,
                                                   tp->partition);
        rktpar->offset = tp->offset;
        rd_kafka_topic_partition_set_leader_epoch(rktpar, tp->leader_epoch);

        Py_BEGIN_ALLOW_THREADS;
        error = rd_kafka_seek_partitions(self->rk, seek_partitions, -1);
        Py_END_ALLOW_THREADS;

        if (error) {
                err = rd_kafka_error_code(error);
                rd_kafka_error_destroy(error);
        }

        if (!err && seek_partitions->elems[0].err) {
                err = seek_partitions->elems[0].err;
        }

        rd_kafka_topic_partition_list_destroy(seek_partitions);

        if (err) {
                cfl_PyErr_Format(err,
                                 "Failed to seek to offset %" CFL_PRId64 ": %s",
                                 tp->offset, rd_kafka_err2str(err));
                goto done;
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Identity-carrying entry point for get_watermark_offsets().
 */
static PyObject *
Consumer__get_watermark_offsets_internal(Handle *self, PyObject *args,
                                         PyObject *kwargs) {

        TopicPartition *tp;
        PyObject *result = NULL;
        rd_kafka_resp_err_t err;
        double tmout = -1.0f;
        int cached   = 0;
        int64_t low = RD_KAFKA_OFFSET_INVALID, high = RD_KAFKA_OFFSET_INVALID;
        static char *kws[] = {"identity", "partition", "timeout", "cached",
                              NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO|db", kws, &identity,
                                         (PyObject **)&tp, &tmout, &cached)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }


        if (PyObject_Type((PyObject *)tp) != (PyObject *)&TopicPartitionType) {
                PyErr_Format(PyExc_TypeError, "expected %s",
                             TopicPartitionType.tp_name);
                goto done;
        }

        if (cached) {
                err = rd_kafka_get_watermark_offsets(
                    self->rk, tp->topic, tp->partition, &low, &high);
        } else {
                Py_BEGIN_ALLOW_THREADS;
                err = rd_kafka_query_watermark_offsets(
                    self->rk, tp->topic, tp->partition, &low, &high,
                    cfl_timeout_ms(tmout));
                Py_END_ALLOW_THREADS;
        }

        if (err) {
                cfl_PyErr_Format(err, "Failed to get watermark offsets: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        result = PyTuple_New(2);
        PyTuple_SetItem(result, 0, PyLong_FromLongLong(low));
        PyTuple_SetItem(result, 1, PyLong_FromLongLong(high));

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Identity-carrying entry point for offsets_for_times().
 */
static PyObject *
Consumer__offsets_for_times_internal(Handle *self, PyObject *args,
                                     PyObject *kwargs) {
#if RD_KAFKA_VERSION < 0x000b0000
        PyErr_Format(PyExc_NotImplementedError,
                     "Consumer offsets_for_times require "
                     "confluent-kafka-python built for librdkafka "
                     "version >=v0.11.0 (librdkafka runtime 0x%x, "
                     "buildtime 0x%x)",
                     rd_kafka_version(), RD_KAFKA_VERSION);
        return NULL;
#else

        PyObject *plist;
        PyObject *result = NULL;
        double tmout = -1.0f;
        rd_kafka_topic_partition_list_t *c_parts;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"identity", "partitions", "timeout", NULL};
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kO|d", kws, &identity,
                                         &plist, &tmout)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(c_parts = py_to_c_parts(plist)))
                goto done;

        Py_BEGIN_ALLOW_THREADS;
        err = rd_kafka_offsets_for_times(self->rk, c_parts,
                                         cfl_timeout_ms(tmout));
        Py_END_ALLOW_THREADS;

        if (err) {
                rd_kafka_topic_partition_list_destroy(c_parts);
                cfl_PyErr_Format(err, "Failed to get offsets: %s",
                                 rd_kafka_err2str(err));
                goto done;
        }

        result = c_parts_to_py(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);

done:
        Handle_gate_exit(self);
        return result;
#endif
}

/**
 * @brief Poll for a single message from the subscribed topics.
 *
 * Instead of a single blocking call to rd_kafka_consumer_poll() with the
 * full timeout, this function:
 * 1. Splits the timeout into 200ms chunks
 * 2. Calls rd_kafka_consumer_poll() with chunk timeout
 * 3. Between chunks, re-acquires GIL and calls PyErr_CheckSignals()
 * 4. If signal detected, returns NULL (raises KeyboardInterrupt)
 * 5. Continues until message received, timeout expired, or signal detected
 *
 *
 * @param self Consumer handle
 * @param args Positional arguments (unused)
 * @param kwargs Keyword arguments:
 *              - identity (unsigned long): gate identity.
 *              - timeout (float, optional): Timeout in seconds.
 *                Default: -1.0 (infinite timeout)
 * @return PyObject* Message object, None if timeout, or NULL on error
 *         (raises KeyboardInterrupt if signal detected)
 */
static PyObject *
Consumer__poll_internal(Handle *self, PyObject *args, PyObject *kwargs) {
        double tmout            = -1.0f;
        static char *kws[]      = {"identity", "timeout", NULL};
        rd_kafka_message_t *rkm = NULL;
        PyObject *result = NULL;
        CallState cs;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms;
        int chunk_timeout_ms;
        int chunk_count = 0;
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "k|d", kws, &identity,
                                         &tmout)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        total_timeout_ms = cfl_timeout_ms(tmout);

        CallState_begin(self, &cs);

        /* Skip wakeable poll pattern for non-blocking or very short timeouts.
         * This avoids unnecessary GIL re-acquisition that can interfere with
         * ThreadPool. Only use wakeable poll for
         * blocking calls that need to be interruptible. */
        if (total_timeout_ms >= 0 && total_timeout_ms < CHUNK_TIMEOUT_MS) {
                rkm = rd_kafka_consumer_poll(self->rk, total_timeout_ms);
        } else {
                while (1) {
                        /* Calculate timeout for this chunk */
                        chunk_timeout_ms = calculate_chunk_timeout(
                            total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);
                        if (chunk_timeout_ms == 0) {
                                /* Timeout expired */
                                break;
                        }

                        /* Poll with chunk timeout */
                        rkm =
                            rd_kafka_consumer_poll(self->rk, chunk_timeout_ms);

                        /* If we got a message, exit the loop */
                        if (rkm) {
                                break;
                        }

                        chunk_count++;

                        /* Check for signals between chunks */
                        if (check_signals_between_chunks(self, &cs))
                                goto done;
                }
        }

        /* Final GIL restore and signal check */
        if (!CallState_end(self, &cs)) {
                if (rkm) {
                        rd_kafka_message_destroy(rkm);
                }
                goto done;
        }

        /* Handle the message */
        if (!rkm) {
                Py_INCREF(Py_None);
                result = Py_None;
                goto done;
        }

        result = Message_new0(self, rkm);
#ifdef RD_KAFKA_V_HEADERS
        /** Have to detach headers outside Message_new0 because it declares the
         * rk message as a const */
        rd_kafka_message_detach_headers(rkm, &((Message *)result)->c_headers);
#endif
        rd_kafka_message_destroy(rkm);

done:
        Handle_gate_exit(self);
        return result;
}


/**
 * @brief Identity-carrying entry point for memberid().
 */
static PyObject *
Consumer__memberid_internal(Handle *self, PyObject *args) {
        char *memberid;
        PyObject *result = NULL;
        atomic_ulong_t identity;

        if (!PyArg_ParseTuple(args, "k", &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        memberid = rd_kafka_memberid(self->rk);

        if (!memberid) {
                Py_INCREF(Py_None);
                result = Py_None;
                goto done;
        }

        if (!*memberid) {
                rd_kafka_mem_free(self->rk, memberid);
                Py_INCREF(Py_None);
                result = Py_None;
                goto done;
        }

        result = Py_BuildValue("s", memberid);
        rd_kafka_mem_free(self->rk, memberid);

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Consume a batch of messages from the subscribed topics.
 *
 * Instead of a single blocking call to rd_kafka_consume_batch_queue() with the
 * full timeout, this function:
 * 1. Splits the timeout into 200ms chunks
 * 2. Calls rd_kafka_consume_batch_queue() with chunk timeout
 * 3. Between chunks, re-acquires GIL and calls PyErr_CheckSignals()
 * 4. If signal detected, returns NULL (raises KeyboardInterrupt)
 * 5. Continues until messages received, timeout expired, or signal detected.
 *
 * @param self Consumer handle
 * @param args Positional arguments (unused)
 * @param kwargs Keyword arguments:
 *              - identity (unsigned long): gate identity.
 *              - num_messages (int, optional): Maximum number of messages to
 *                consume per call. Default: 1. Maximum: 1000000.
 *              - timeout (float, optional): Timeout in seconds.
 *                Default: -1.0 (infinite timeout)
 * @return PyObject* List of Message objects, empty list if timeout, or NULL on
 * error (raises KeyboardInterrupt if signal detected)
 */
static PyObject *
Consumer__consume_internal(Handle *self, PyObject *args, PyObject *kwargs) {
        unsigned int num_messages = 1;
        double tmout              = -1.0f;
        static char *kws[]        = {"identity", "num_messages", "timeout",
                              NULL};
        rd_kafka_message_t **rkmessages;
        PyObject *msglist;
        rd_kafka_queue_t *rkqu = self->u.Consumer.rkqu;
        CallState cs;
        Py_ssize_t i, n = 0;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms;
        int chunk_timeout_ms;
        int chunk_count = 0;
        atomic_ulong_t identity;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "k|Id", kws, &identity,
                                         &num_messages, &tmout)) {
                return NULL;
        }

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                Handle_gate_exit(self);
                return NULL;
        }

        if (num_messages > 1000000) {
                PyErr_SetString(
                    PyExc_ValueError,
                    "num_messages must be between 0 and 1000000 (1M)");
                Handle_gate_exit(self);
                return NULL;
        }

        total_timeout_ms = cfl_timeout_ms(tmout);

        rkmessages = malloc(num_messages * sizeof(rd_kafka_message_t *));
        if (!rkmessages) {
                PyErr_NoMemory();
                Handle_gate_exit(self);
                return NULL;
        }

        CallState_begin(self, &cs);

        /* Skip wakeable poll pattern for non-blocking or very short timeouts.
         * This avoids unnecessary GIL re-acquisition that can interfere with
         * ThreadPool. Only use wakeable poll for
         * blocking calls that need to be interruptible. */
        if (total_timeout_ms >= 0 && total_timeout_ms < CHUNK_TIMEOUT_MS) {
                n = (Py_ssize_t)rd_kafka_consume_batch_queue(
                    rkqu, total_timeout_ms, rkmessages, num_messages);

                if (n < 0) {
                        /* Error - need to restore GIL before setting error */
                        PyEval_RestoreThread(cs.thread_state);
                        free(rkmessages);
                        cfl_PyErr_Format(
                            rd_kafka_last_error(), "%s",
                            rd_kafka_err2str(rd_kafka_last_error()));
                        Handle_gate_exit(self);
                        return NULL;
                }
        } else {
                while (1) {
                        /* Calculate timeout for this chunk */
                        chunk_timeout_ms = calculate_chunk_timeout(
                            total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);
                        if (chunk_timeout_ms == 0) {
                                /* Timeout expired */
                                break;
                        }

                        /* Consume with chunk timeout */
                        n = (Py_ssize_t)rd_kafka_consume_batch_queue(
                            rkqu, chunk_timeout_ms, rkmessages, num_messages);

                        if (n < 0) {
                                /* Error - need to restore GIL before setting
                                 * error */
                                PyEval_RestoreThread(cs.thread_state);
                                free(rkmessages);
                                cfl_PyErr_Format(
                                    rd_kafka_last_error(), "%s",
                                    rd_kafka_err2str(rd_kafka_last_error()));
                                Handle_gate_exit(self);
                                return NULL;
                        }

                        /* If we got messages, exit the loop */
                        if (n > 0) {
                                break;
                        }

                        chunk_count++;

                        /* Check for signals between chunks */
                        if (check_signals_between_chunks(self, &cs)) {
                                free(rkmessages);
                                Handle_gate_exit(self);
                                return NULL;
                        }
                }
        }

        /* Final GIL restore and signal check */
        if (!CallState_end(self, &cs)) {
                for (i = 0; i < n; i++) {
                        rd_kafka_message_destroy(rkmessages[i]);
                }
                free(rkmessages);
                Handle_gate_exit(self);
                return NULL;
        }

        /* Create Python list from messages  */
        msglist = PyList_New(n);

        for (i = 0; i < n; i++) {
                PyObject *msgobj = Message_new0(self, rkmessages[i]);
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

        Handle_gate_exit(self);
        return msglist;
}


/**
 * @brief Identity-carrying entry point for close().
 */
static PyObject *Consumer__close_internal(Handle *self, PyObject *args) {
        CallState cs;
        PyObject *result = NULL;
        atomic_ulong_t identity;

        if (!PyArg_ParseTuple(args, "k", &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                Py_INCREF(Py_None);
                result = Py_None;
                goto done;
        }

        CallState_begin(self, &cs);

        rd_kafka_consumer_close(self->rk);

        if (self->u.Consumer.rkqu) {
                rd_kafka_queue_destroy(self->u.Consumer.rkqu);
                self->u.Consumer.rkqu = NULL;
        }

        rd_kafka_destroy(self->rk);
        self->rk = NULL;

        if (!CallState_end(self, &cs))
                goto done;

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

static PyObject *Consumer_enter(Handle *self) {
        atomic_ulong_t identity = (atomic_ulong_t)PyThread_get_thread_ident();

        if (!Handle_gate_enter(self, identity))
                return NULL;
        Py_INCREF(self);
        Handle_gate_exit(self);
        return (PyObject *)self;
}

static PyObject *Consumer_exit(Handle *self, PyObject *args) {
        PyObject *exc_type, *exc_value, *exc_traceback;
        PyObject *result = NULL;
        atomic_ulong_t identity = (atomic_ulong_t)PyThread_get_thread_ident();

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!PyArg_UnpackTuple(args, "__exit__", 3, 3, &exc_type, &exc_value,
                               &exc_traceback))
                goto done;

        if (self->rk) {
                PyObject *close_args = Py_BuildValue("(k)", identity);
                PyObject *close_result;

                if (!close_args)
                        goto done;

                close_result = Consumer__close_internal(self, close_args);
                Py_DECREF(close_args);
                if (!close_result)
                        goto done;
                Py_DECREF(close_result);
        }

        Py_INCREF(Py_None);
        result = Py_None;

done:
        Handle_gate_exit(self);
        return result;
}

/**
 * @brief Identity-carrying entry point for consumer_group_metadata().
 */
static PyObject *
Consumer__consumer_group_metadata_internal(Handle *self, PyObject *args) {
        rd_kafka_consumer_group_metadata_t *cgmd;
        PyObject *result = NULL;
        atomic_ulong_t identity;

        if (!PyArg_ParseTuple(args, "k", &identity))
                return NULL;

        if (!Handle_gate_enter(self, identity))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_CONSUMER_CLOSED);
                goto done;
        }

        if (!(cgmd = rd_kafka_consumer_group_metadata(self->rk))) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Consumer group metadata not available");
                goto done;
        }

        result = c_cgmd_to_py(cgmd);

        rd_kafka_consumer_group_metadata_destroy(cgmd);

done:
        Handle_gate_exit(self);
        return result; /* Possibly NULL */
}

/****************************************************************************
 *
 *
 * Public APIs of sync Consumer which wrap over the identity-carrying _internal
 * methods.
 *
 * Every function in this section is a thin wrapper, expected to be called
 * only by the sync Consumer (never AIOConsumer): each one passes the
 * calling thread's own ID through to its corresponding
 * Consumer__<method>_internal()  counterpart.
 *
 *
 ****************************************************************************/

/**
 * @brief Build a new args tuple with the calling thread's own ID prepended
 *        to `orig_args`, for a public Consumer_<method>() wrapper to call
 *        straight through to its Consumer__<method>_internal() counterpart.
 *        Prepended, not appended because identity is mandatory while the
 *        Python-facing args that follow it may be optional, and
 *        PyArg_ParseTupleAndKeywords only supports required arguments before
 *        optional ones.
 *        Returns a new reference, or NULL with an exception set.
 */
static PyObject *Consumer_prepend_thread_id(PyObject *orig_args) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        Py_ssize_t nargs       = PyTuple_GET_SIZE(orig_args);
        Py_ssize_t i;
        PyObject *full_args = PyTuple_New(nargs + 1);

        if (!full_args)
                return NULL;

        PyTuple_SET_ITEM(full_args, 0, PyLong_FromUnsignedLong(identity));
        if (!PyTuple_GET_ITEM(full_args, 0)) {
                Py_DECREF(full_args);
                return NULL;
        }
        for (i = 0; i < nargs; i++) {
                PyObject *item = PyTuple_GET_ITEM(orig_args, i);
                Py_INCREF(item);
                PyTuple_SET_ITEM(full_args, i + 1, item);
        }

        return full_args;
}

static PyObject *Consumer_assign(Handle *self, PyObject *tlist) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("kO", identity, tlist);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__assign_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *Consumer_incremental_assign(Handle *self, PyObject *tlist) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("kO", identity, tlist);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__incremental_assign_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *Consumer_unassign(Handle *self, PyObject *ignore) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("(k)", identity);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__unassign_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *Consumer_incremental_unassign(Handle *self,
                                               PyObject *tlist) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("kO", identity, tlist);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__incremental_unassign_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *
Consumer_commit(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__commit_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_subscribe(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__subscribe_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *Consumer_unsubscribe(Handle *self, PyObject *ignore) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("(k)", identity);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__unsubscribe_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *
Consumer_assignment(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__assignment_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_store_offsets(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__store_offsets_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_committed(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__committed_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_position(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__position_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_pause(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__pause_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_resume(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__resume_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *Consumer_seek(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__seek_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_get_watermark_offsets(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__get_watermark_offsets_internal(self, full_args,
                                                           kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_offsets_for_times(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__offsets_for_times_internal(self, full_args,
                                                       kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *Consumer_poll(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__poll_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *
Consumer_memberid(Handle *self, PyObject *ignore) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("(k)", identity);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__memberid_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *
Consumer_consume(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *full_args = Consumer_prepend_thread_id(args);
        PyObject *result;

        if (!full_args)
                return NULL;

        result = Consumer__consume_internal(self, full_args, kwargs);
        Py_DECREF(full_args);
        return result;
}

static PyObject *Consumer_close(Handle *self, PyObject *ignore) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("(k)", identity);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__close_internal(self, args);
        Py_DECREF(args);
        return result;
}

static PyObject *Consumer_consumer_group_metadata(Handle *self,
                                                  PyObject *ignore) {
        unsigned long identity = (unsigned long)PyThread_get_thread_ident();
        PyObject *args         = Py_BuildValue("(k)", identity);
        PyObject *result;

        if (!args)
                return NULL;

        result = Consumer__consumer_group_metadata_internal(self, args);
        Py_DECREF(args);
        return result;
}

/****************************************************************************
 *
 *
 * End of public APIs of sync Consumer
 *
 *
 ****************************************************************************/


static PyMethodDef Consumer_methods[] = {
    {"subscribe", (PyCFunction)Consumer_subscribe, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: subscribe(topics, [on_assign=None], [on_revoke=None], "
     "[on_lost=None])\n"
     "\n"
     "  Set subscription to supplied list of topics\n"
     "  This replaces a previous subscription.\n"
     "\n"
     "  Regexp pattern subscriptions are supported by prefixing "
     "the topic string with ``\"^\"``, e.g.::\n"
     "\n"
     "    consumer.subscribe([\"^my_topic.*\", \"^another[0-9]-?[a-z]+$\", "
     "\"not_a_regex\"])\n"
     "\n"
     "  :param list(str) topics: List of topics (strings) to subscribe to.\n"
     "  :param callable on_assign: callback to provide handling of "
     "customized offsets on completion of a successful partition "
     "re-assignment.\n"
     "  :param callable on_revoke: callback to provide handling of "
     "offset commits to a customized store on the start of a "
     "rebalance operation.\n"
     "  :param callable on_lost: callback to provide handling in "
     "the case the partition assignment has been lost. If not "
     "specified, lost partition events will be delivered to "
     "on_revoke, if specified. Partitions that have been lost may "
     "already be owned by other members in the group and therefore "
     "committing offsets, for example, may fail.\n"
     "\n"
     "  :raises KafkaException:\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"
     "\n"
     ".. py:function:: on_assign(consumer, partitions)\n"
     ".. py:function:: on_revoke(consumer, partitions)\n"
     ".. py:function:: on_lost(consumer, partitions)\n"
     "\n"
     "  :param Consumer consumer: Consumer instance.\n"
     "  :param list(TopicPartition) partitions: Absolute list of partitions "
     "being assigned or revoked.\n"
     "\n"},
    {"unsubscribe", (PyCFunction)Consumer_unsubscribe, METH_NOARGS,
     "  Remove current subscription.\n"
     "\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"poll", (PyCFunction)Consumer_poll, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: poll([timeout=None])\n"
     "\n"
     "  Consumes a single message, calls callbacks and returns events.\n"
     "\n"
     "  The application must check the returned :py:class:`Message` "
     "object's :py:func:`Message.error()` method to distinguish "
     "between proper messages (error() returns None), or an event or "
     "error (see error().code() for specifics).\n"
     "\n"
     "  .. note: Callbacks may be called from this method, "
     "such as ``on_assign``, ``on_revoke``, et.al.\n"
     "\n"
     "  :param float timeout: Maximum time to block waiting for message, event "
     "or callback (default: infinite (None translated into -1 in the "
     "library)). (Seconds)\n"
     "  :returns: A Message object or None on timeout\n"
     "  :rtype: :py:class:`Message` or None\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"consume", (PyCFunction)Consumer_consume, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: consume([num_messages=1], [timeout=-1])\n"
     "\n"
     "  Consumes a list of messages (possibly empty on timeout). "
     "Callbacks may be executed as a side effect of calling this method.\n"
     "\n"
     "  The application must check the returned :py:class:`Message` "
     "object's :py:func:`Message.error()` method to distinguish "
     "between proper messages (error() returns None) and errors "
     "for each :py:class:`Message` in the list (see error().code() "
     "for specifics). If the enable.partition.eof configuration "
     "property is set to True, partition EOF events will also be "
     "exposed as Messages with error().code() set to "
     "_PARTITION_EOF.\n"
     "\n"
     "  .. note: Callbacks may be called from this method, "
     "such as ``on_assign``, ``on_revoke``, et.al.\n"
     "\n"
     "  :param int num_messages: The maximum number of messages to return "
     "(default: 1).\n"
     "  :param float timeout: The maximum time to block waiting for message, "
     "event or callback (default: infinite (-1)). (Seconds)\n"
     "  :returns: A list of Message objects (possibly empty on timeout)\n"
     "  :rtype: list(Message)\n"
     "  :raises RuntimeError: if called on a closed consumer\n"
     "  :raises KafkaError: in case of internal error\n"
     "  :raises ValueError: if num_messages > 1M\n"
     "\n"},
    {"assign", (PyCFunction)Consumer_assign, METH_O,
     ".. py:function:: assign(partitions)\n"
     "\n"
     "  Set the consumer partition assignment to the provided list of "
     ":py:class:`TopicPartition` and start consuming.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions and "
     "optionally initial offsets to start consuming from.\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"incremental_assign", (PyCFunction)Consumer_incremental_assign, METH_O,
     ".. py:function:: incremental_assign(partitions)\n"
     "\n"
     "  Incrementally add the provided list of :py:class:`TopicPartition` "
     "to the current partition assignment. This list must not contain "
     "duplicate entries, or any entry corresponding to an already "
     "assigned partition. When a COOPERATIVE assignor (i.e. incremental "
     "rebalancing) is being used, this method may be used in the on_assign "
     "callback to update the current assignment and specify start offsets. "
     "The application should pass a list of partitions identical to the "
     "list passed to the callback, even if the list is empty. Note that if "
     "you do not call incremental_assign in your on_assign handler, this "
     "will be done automatically and start offsets will be the last committed "
     "offsets, or determined via the auto offset reset policy "
     "(auto.offset.reset) if there "
     "are none. This method may also be used outside the context of a "
     "rebalance callback.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions and "
     "optionally initial offsets to start consuming from.\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"unassign", (PyCFunction)Consumer_unassign, METH_NOARGS,
     "  Removes the current partition assignment and stops consuming.\n"
     "\n"
     "  :raises KafkaException:\n"
     "  :raises RuntimeError: if called on a closed consumer\n"
     "\n"},
    {"incremental_unassign", (PyCFunction)Consumer_incremental_unassign, METH_O,
     ".. py:function:: incremental_unassign(partitions)\n"
     "\n"
     "  Incrementally remove the provided list of :py:class:`TopicPartition` "
     "from the current partition assignment. This list must not contain "
     "dupliate entries and all entries specified must be part of the "
     "current assignment. When a COOPERATIVE assignor (i.e. incremental "
     "rebalancing) is being used, this method may be used in the on_revoke "
     "or on_lost callback to update the current assignment. The application "
     "should pass a list of partitions identical to the list passed to the "
     "callback. This method may also be used outside the context of a "
     "rebalance callback. The value of the `TopicPartition` offset field "
     "is ignored by this method.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions to "
     "remove from the current assignment.\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"assignment", (PyCFunction)Consumer_assignment,
     METH_VARARGS | METH_KEYWORDS,
     "  Returns the current partition assignment.\n"
     "\n"
     "  :returns: List of assigned topic+partitions.\n"
     "  :rtype: list(TopicPartition)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"store_offsets", (PyCFunction)Consumer_store_offsets,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: store_offsets([message=None], [offsets=None])\n"
     "\n"
     "  Store offsets for a message or a list of offsets.\n"
     "\n"
     "  ``message`` and ``offsets`` are mutually exclusive. "
     "The stored offsets will be committed according to "
     "'auto.commit.interval.ms' or manual "
     "offset-less :py:meth:`commit`. "
     "Note that 'enable.auto.offset.store' must be set to False when using "
     "this API.\n"
     "\n"
     "  :param confluent_kafka.Message message: Store message's offset+1.\n"
     "  :param list(TopicPartition) offsets: List of topic+partitions+offsets "
     "to store.\n"
     "  :rtype: None\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"commit", (PyCFunction)Consumer_commit, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: commit([message=None], [offsets=None], "
     "[asynchronous=True])\n"
     "\n"
     "  Commit a message or a list of offsets.\n"
     "\n"
     "  The ``message`` and ``offsets`` parameters are mutually exclusive. If "
     "neither is set, "
     "the current partition assignment's offsets are used instead. "
     "Use this method to commit offsets if you have 'enable.auto.commit' set "
     "to False.\n"
     "\n"
     "  :param confluent_kafka.Message message: Commit the message's offset+1. "
     "Note: "
     "By convention, committed offsets reflect the next message to be "
     "consumed, **not** "
     "the last message consumed.\n"
     "  :param list(TopicPartition) offsets: List of topic+partitions+offsets "
     "to commit.\n"
     "  :param bool asynchronous: If true, asynchronously commit, returning "
     "None immediately. "
     "If False, the commit() call will block until the commit succeeds or "
     "fails and the committed offsets will be returned (on success). Note that "
     "specific partitions may have failed and the .err field of each partition "
     "should be checked for success.\n"
     "  :rtype: None|list(TopicPartition)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"committed", (PyCFunction)Consumer_committed, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: committed(partitions, [timeout=None])\n"
     "\n"
     "  Retrieve committed offsets for the specified partitions.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions "
     "to query for stored offsets.\n"
     "  :param float timeout: Request timeout (seconds).\n"
     "  :returns: List of topic+partitions with offset and possibly error "
     "set.\n"
     "  :rtype: list(TopicPartition)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"position", (PyCFunction)Consumer_position, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: position(partitions)\n"
     "\n"
     "  Retrieve current positions (offsets) for the specified partitions.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions "
     "to return current offsets for. The current offset is the offset of the "
     "last consumed message + 1.\n"
     "  :returns: List of topic+partitions with offset and possibly error "
     "set.\n"
     "  :rtype: list(TopicPartition)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"pause", (PyCFunction)Consumer_pause, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: pause(partitions)\n"
     "\n"
     "  Pause consumption for the provided list of partitions.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions "
     "to pause.\n"
     "  :rtype: None\n"
     "  :raises: KafkaException\n"
     "\n"},
    {"resume", (PyCFunction)Consumer_resume, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: resume(partitions)\n"
     "\n"
     "  Resume consumption for the provided list of partitions.\n"
     "\n"
     "  :param list(TopicPartition) partitions: List of topic+partitions "
     "to resume.\n"
     "  :rtype: None\n"
     "  :raises: KafkaException\n"
     "\n"},
    {"seek", (PyCFunction)Consumer_seek, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: seek(partition)\n"
     "\n"
     "  Set consume position for partition to offset.\n"
     "  The offset may be an absolute (>=0) or a\n"
     "  logical offset (:py:const:`OFFSET_BEGINNING` et.al).\n"
     "\n"
     "  seek() may only be used to update the consume offset of an\n"
     "  actively consumed partition (i.e., after :py:const:`assign()`),\n"
     "  to set the starting offset of partition not being consumed instead\n"
     "  pass the offset in an `assign()` call.\n"
     "\n"
     "  :param TopicPartition partition: Topic+partition+offset to seek to.\n"
     "\n"
     "  :raises: KafkaException\n"
     "\n"},
    {"get_watermark_offsets", (PyCFunction)Consumer_get_watermark_offsets,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: get_watermark_offsets(partition, [timeout=None], "
     "[cached=False])\n"
     "\n"
     "  Retrieve low and high offsets for the specified partition.\n"
     "\n"
     "  :param TopicPartition partition: Topic+partition to return offsets "
     "for.\n"
     "  :param float timeout: Request timeout (seconds). Ignored if "
     "cached=True.\n"
     "  :param bool cached: Instead of querying the broker, use cached "
     "information. "
     "Cached values: The low offset is updated periodically (if "
     "statistics.interval.ms is set) while "
     "the high offset is updated on each message fetched from the broker for "
     "this partition.\n"
     "  :returns: Tuple of (low,high) on success or None on timeout. "
     "The high offset is the offset of the last message + 1.\n"
     "  :rtype: tuple(int,int)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"offsets_for_times", (PyCFunction)Consumer_offsets_for_times,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: offsets_for_times(partitions, [timeout=None])\n"
     "\n"
     " Look up offsets by timestamp for the specified partitions.\n"
     "\n"
     " The returned offset for each partition is the earliest offset whose\n"
     " timestamp is greater than or equal to the given timestamp in the\n"
     " corresponding partition. If the provided timestamp exceeds that of the\n"
     " last message in the partition, a value of -1 will be returned.\n"
     "\n"
     "  :param list(TopicPartition) partitions: topic+partitions with "
     "timestamps in the TopicPartition.offset field.\n"
     "  :param float timeout: Request timeout (seconds).\n"
     "  :returns: List of topic+partition with offset field set and possibly "
     "error set\n"
     "  :rtype: list(TopicPartition)\n"
     "  :raises: KafkaException\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"memberid", (PyCFunction)Consumer_memberid, METH_NOARGS,
     ".. py:function:: memberid()\n"
     "\n"
     " Return this client's broker-assigned group member id.\n"
     "\n"
     " The member id is assigned by the group coordinator and"
     " is propagated to the consumer during rebalance.\n"
     "\n"
     "  :returns: Member id string or None\n"
     "  :rtype: string\n"
     "  :raises: RuntimeError if called on a closed consumer\n"
     "\n"},
    {"close", (PyCFunction)Consumer_close, METH_NOARGS,
     "\n"
     "  Close down and terminate the Kafka Consumer.\n"
     "\n"
     "  Actions performed:\n"
     "\n"
     "  - Stops consuming.\n"
     "  - Commits offsets, unless the consumer property 'enable.auto.commit' "
     "is set to False.\n"
     "  - Leaves the consumer group.\n"
     "\n"
     "  .. note: Registered callbacks may be called from this method, "
     "see :py:func::`poll()` for more info.\n"
     "\n"
     "  :rtype: None\n"
     "\n"},
    {"list_topics", (PyCFunction)list_topics, METH_VARARGS | METH_KEYWORDS,
     list_topics_doc},
    {"consumer_group_metadata", (PyCFunction)Consumer_consumer_group_metadata,
     METH_NOARGS,
     ".. py:function:: consumer_group_metadata()\n"
     "\n"
     " :returns: An opaque object representing the consumer's current "
     "group metadata for passing to the transactional producer's "
     "send_offsets_to_transaction() API.\n"
     "\n"},
    {"set_sasl_credentials", (PyCFunction)set_sasl_credentials,
     METH_VARARGS | METH_KEYWORDS, set_sasl_credentials_doc},
    {"__enter__", (PyCFunction)Consumer_enter, METH_NOARGS,
     "Context manager entry."},
    {"__exit__", (PyCFunction)Consumer_exit, METH_VARARGS,
     "Context manager exit. Automatically closes the consumer."},
     /* Internal-only, identity-carrying entry points. Called directly by
     * the public sync Consumer_<method>() wrappers and by AIOConsumer.
     */
    {"_assign_internal", (PyCFunction)Consumer__assign_internal,
     METH_VARARGS, "Internal use only."},
    {"_incremental_assign_internal",
     (PyCFunction)Consumer__incremental_assign_internal, METH_VARARGS,
     "Internal use only."},
    {"_unassign_internal", (PyCFunction)Consumer__unassign_internal,
     METH_VARARGS, "Internal use only."},
    {"_incremental_unassign_internal",
     (PyCFunction)Consumer__incremental_unassign_internal, METH_VARARGS,
     "Internal use only."},
    {"_commit_internal", (PyCFunction)Consumer__commit_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_subscribe_internal", (PyCFunction)Consumer__subscribe_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_unsubscribe_internal", (PyCFunction)Consumer__unsubscribe_internal,
     METH_VARARGS, "Internal use only."},
    {"_assignment_internal", (PyCFunction)Consumer__assignment_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_store_offsets_internal", (PyCFunction)Consumer__store_offsets_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_committed_internal", (PyCFunction)Consumer__committed_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_position_internal", (PyCFunction)Consumer__position_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_pause_internal", (PyCFunction)Consumer__pause_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_resume_internal", (PyCFunction)Consumer__resume_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_seek_internal", (PyCFunction)Consumer__seek_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_get_watermark_offsets_internal",
     (PyCFunction)Consumer__get_watermark_offsets_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_offsets_for_times_internal",
     (PyCFunction)Consumer__offsets_for_times_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_poll_internal", (PyCFunction)Consumer__poll_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_memberid_internal", (PyCFunction)Consumer__memberid_internal,
     METH_VARARGS, "Internal use only."},
    {"_consume_internal", (PyCFunction)Consumer__consume_internal,
     METH_VARARGS | METH_KEYWORDS, "Internal use only."},
    {"_close_internal", (PyCFunction)Consumer__close_internal, METH_VARARGS,
     "Internal use only."},
    {"_consumer_group_metadata_internal",
     (PyCFunction)Consumer__consumer_group_metadata_internal, METH_VARARGS,
     "Internal use only."},
    {NULL}};


static void Consumer_rebalance_cb(rd_kafka_t *rk,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *c_parts,
                                  void *opaque) {
        Handle *self = opaque;
        CallState *cs;
        PyObject *cb;

        cs = CallState_get(self);

        self->u.Consumer.rebalance_assigned               = 0;
        self->u.Consumer.rebalance_incremental_assigned   = 0;
        self->u.Consumer.rebalance_incremental_unassigned = 0;

        if ((err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS &&
             self->u.Consumer.on_assign) ||
            (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS &&
             self->u.Consumer.on_revoke) ||
            (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS &&
             self->u.Consumer.on_lost && rd_kafka_assignment_lost(rk))) {

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

                if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                        cb = self->u.Consumer.on_assign;
                else if (rd_kafka_assignment_lost(rk) &&
                         self->u.Consumer.on_lost)
                        cb = self->u.Consumer.on_lost;
                else /* revoke */
                        cb = self->u.Consumer.on_revoke;

                result = PyObject_CallObject(cb, args);

                Py_DECREF(args);

                if (result)
                        Py_DECREF(result);
                else {
                        CallState_fetch_exception(cs);
                        CallState_crash(cs);
                        rd_kafka_yield(rk);
                }
        }

        /* Fallback: librdkafka needs the rebalance_cb to call assign()
         * to synchronize state, if the user did not do this from callback,
         * or there was no callback, or the callback failed, then we perform
         * that assign() call here instead. */
        if (!(self->u.Consumer.rebalance_assigned ||
              self->u.Consumer.rebalance_incremental_assigned ||
              self->u.Consumer.rebalance_incremental_unassigned)) {
                const char *rebalance_protocol =
                    rd_kafka_rebalance_protocol(rk);
                if (rebalance_protocol &&
                    !strcmp(rebalance_protocol, "COOPERATIVE")) {
                        rd_kafka_error_t *error = NULL;

                        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                                error =
                                    rd_kafka_incremental_assign(rk, c_parts);
                        else
                                error =
                                    rd_kafka_incremental_unassign(rk, c_parts);

                        if (error) {
                                cfl_PyErr_from_error_destroy(error);
                                CallState_crash(cs);
                        }

                } else {
                        rd_kafka_resp_err_t assign_err;

                        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                                assign_err = rd_kafka_assign(rk, c_parts);
                        else
                                assign_err = rd_kafka_assign(rk, NULL);

                        if (assign_err) {
                                cfl_PyErr_Format(assign_err,
                                                 "Partition assignment failed");
                                CallState_crash(cs);
                        }
                }
        }

        CallState_resume(cs);
}



static int Consumer_init(PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Consumer already initialized");
                return -1;
        }

        self->type = RD_KAFKA_CONSUMER;

        if (!(conf = common_conf_setup(RD_KAFKA_CONSUMER, self, args, kwargs)))
                return -1; /* Exception raised by ..conf_setup() */

        rd_kafka_conf_set_rebalance_cb(conf, Consumer_rebalance_cb);
        rd_kafka_conf_set_offset_commit_cb(conf, Consumer_offset_commit_cb);

        self->rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create consumer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Enable Token Refresh to be handled by background thread if OAuth
         * callback is provided */
        if (self->oauth_cb) {
                rd_kafka_sasl_background_callbacks_enable(self->rk);
        }

        /* Forward log messages to main queue which is then forwarded
         * to the consumer queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        rd_kafka_poll_set_consumer(self->rk);

        self->u.Consumer.rkqu = rd_kafka_queue_get_consumer(self->rk);
        assert(self->u.Consumer.rkqu);


        /* Wait for the background thread to set the token. Caller owns
         * destroy on failure — wait_for_oauth_token_set no longer touches
         * self->rk (see refactor note in confluent_kafka.c). */
        if (self->oauth_cb) {
                int ret_wait_oauth = wait_for_oauth_token_set(self);
                if (ret_wait_oauth == -1) {
                        CallState cs;
                        CallState_begin(self, &cs);
                        rd_kafka_destroy(self->rk);
                        CallState_end(self, &cs);
                        self->rk = NULL;
                }
                return ret_wait_oauth;
        }

        return 0;
}

static PyObject *
Consumer_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}


PyTypeObject ConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "cimpl.Consumer", /*tp_name*/
    sizeof(Handle),                                  /*tp_basicsize*/
    0,                                               /*tp_itemsize*/
    (destructor)Consumer_dealloc,                    /*tp_dealloc*/
    0,                                               /*tp_print*/
    0,                                               /*tp_getattr*/
    0,                                               /*tp_setattr*/
    0,                                               /*tp_compare*/
    0,                                               /*tp_repr*/
    0,                                               /*tp_as_number*/
    0,                                               /*tp_as_sequence*/
    0,                                               /*tp_as_mapping*/
    0,                                               /*tp_hash */
    0,                                               /*tp_call*/
    0,                                               /*tp_str*/
    0,                                               /*tp_getattro*/
    0,                                               /*tp_setattro*/
    0,                                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "A high-level Apache Kafka consumer\n"
    "\n"
    ".. py:function:: Consumer(config)\n"
    "\n"
    "Create a new Consumer instance using the provided configuration *dict* ("
    "including properties and callback functions). "
    "See :ref:`pythonclient_configuration` for more information."
    "\n\n"
    ":param dict config: Configuration properties. At a minimum, "
    "``group.id`` **must** be set and ``bootstrap.servers`` **should** be set."
    "\n",                            /*tp_doc*/
    (traverseproc)Consumer_traverse, /* tp_traverse */
    (inquiry)Consumer_clear,         /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Consumer_methods,                /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    Consumer_init,                   /* tp_init */
    0,                               /* tp_alloc */
    Consumer_new                     /* tp_new */
};
