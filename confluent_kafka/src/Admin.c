/**
 * Copyright 2018 Confluent Inc.
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


/****************************************************************************
 *
 *
 * Admin Client API
 *
 *
 ****************************************************************************/




static int Admin_clear (Handle *self) {

        Handle_clear(self);

        return 0;
}

static void Admin_dealloc (Handle *self) {
        PyObject_GC_UnTrack(self);

        Admin_clear(self);

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Admin_traverse (Handle *self,
                           visitproc visit, void *arg) {
        Handle_traverse(self, visit, arg);

        return 0;
}


/**
 * @name AdminOptions
 *
 *
 */
#define Admin_options_def_int   (-12345)
#define Admin_options_def_float ((float)Admin_options_def_int)

struct Admin_options {
        int   validate_only;      /* parser: i */
        float request_timeout;    /* parser: f */
        float operation_timeout;  /* parser: f */
        int   broker;             /* parser: i */
};

/**@brief "unset" value initializers for Admin_options
 * Make sure this is kept up to date with Admin_options above. */
#define Admin_options_INITIALIZER {                                     \
                Admin_options_def_int, Admin_options_def_float,         \
                        Admin_options_def_float, Admin_options_def_int \
                        }

#define Admin_options_is_set_int(v) ((v) != Admin_options_def_int)
#define Admin_options_is_set_float(v) Admin_options_is_set_int((int)(v))


/**
 * @brief Convert Admin_options to rd_kafka_AdminOptions_t.
 *
 * @param forApi is the librdkafka name of the admin API that these options
 *               will be used for, e.g., "CreateTopics".
 * @param future is set as the options opaque.
 *
 * @returns a new C admin options object on success, or NULL on failure in
 *          which case an exception is raised.
 */
static rd_kafka_AdminOptions_t *
Admin_options_to_c (Handle *self, const char *forApi,
                    const struct Admin_options *options,
                    PyObject *future) {
        rd_kafka_AdminOptions_t *c_options;
        rd_kafka_resp_err_t err;
        char errstr[512];

        c_options = rd_kafka_AdminOptions_new(self->rk, forApi);
        if (!c_options) {
                PyErr_Format(PyExc_RuntimeError,
                             "Admin API %s unsupported by librdkafka", forApi);
                return NULL;
        }

        rd_kafka_AdminOptions_set_opaque(c_options, (void *)future);

        if (Admin_options_is_set_int(options->validate_only) &&
            (err = rd_kafka_AdminOptions_set_validate_only(
                    c_options, options->validate_only,
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_float(options->request_timeout) &&
            (err = rd_kafka_AdminOptions_set_request_timeout(
                    c_options, (int)(options->request_timeout * 1000.0f),
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_float(options->operation_timeout) &&
            (err = rd_kafka_AdminOptions_set_operation_timeout(
                    c_options, (int)(options->operation_timeout * 1000.0f),
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_int(options->broker) &&
            (err = rd_kafka_AdminOptions_set_broker(
                    c_options, (int32_t)options->broker,
                    errstr, sizeof(errstr))))
                goto err;


        return c_options;

 err:
        rd_kafka_AdminOptions_destroy(c_options);
        PyErr_Format(PyExc_ValueError, "%s", errstr);
        return NULL;
}




/**
 * @brief Translate Python list(list(int)) replica assignments and set
 *        on the specified generic C object using a setter based on
 *        forApi.
 *
 * @returns 1 on success or 0 on error in which case an exception is raised.
 */
static int Admin_set_replica_assignment (const char *forApi, void *c_obj,
                                         PyObject *ra, int
                                         min_count, int max_count,
                                         const char *err_count_desc) {
        int pi;

        if (!PyList_Check(ra) ||
            (int)PyList_Size(ra) < min_count ||
            (int)PyList_Size(ra) > max_count) {
                PyErr_Format(PyExc_TypeError,
                             "replica_assignment must be "
                             "a list of int lists with an "
                             "outer size of %s", err_count_desc);
                return 0;
        }

        for (pi = 0 ; pi < (int)PyList_Size(ra) ; pi++) {
                size_t ri;
                PyObject *replicas = PyList_GET_ITEM(ra, pi);
                rd_kafka_resp_err_t err;
                int32_t *c_replicas;
                size_t replica_cnt;
                char errstr[512];

                if (!PyList_Check(replicas) ||
                    (replica_cnt = (size_t)PyList_Size(replicas)) < 1) {
                        PyErr_Format(
                                PyExc_TypeError,
                                "replica_assignment must be "
                                "a list of int lists with an "
                                "outer size of %s", err_count_desc);
                        return 0;
                }

                c_replicas = alloca(sizeof(*c_replicas) *
                                    replica_cnt);

                for (ri = 0 ; ri < replica_cnt ; ri++) {
                        PyObject *replica =
                                PyList_GET_ITEM(replicas, ri);
                        if (!PyLong_Check(replica)) {
                                PyErr_Format(
                                        PyExc_TypeError,
                                        "replica_assignment must be "
                                        "a list of int lists with an "
                                        "outer size of %s", err_count_desc);
                                return 0;
                        }

                        c_replicas[ri] =
                                (int32_t)PyLong_AsLong(replica);

                }


                if (!strcmp(forApi, "CreateTopics"))
                        err = rd_kafka_NewTopic_set_replica_assignment(
                                (rd_kafka_NewTopic_t *)c_obj, (int32_t)pi,
                                c_replicas, replica_cnt,
                                errstr, sizeof(errstr));
                else if (!strcmp(forApi, "CreatePartitions"))
                        err = rd_kafka_NewPartitions_set_replica_assignment(
                                (rd_kafka_NewPartitions_t *)c_obj, (int32_t)pi,
                                c_replicas, replica_cnt,
                                errstr, sizeof(errstr));
                else {
                        /* Should never be reached */
                        err = RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
                        snprintf(errstr, sizeof(errstr),
                                 "Unsupported forApi %s", forApi);
                }

                if (err) {
                        PyErr_SetString(
                                PyExc_TypeError, errstr);
                        return 0;
                }
        }

        return 1;
}



/**
 * @brief create_topics
 */
static PyObject *Admin_create_topics (Handle *self, PyObject *args,
                                      PyObject *kwargs) {
        PyObject *topics = NULL, *future;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        rd_kafka_NewTopic_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewTopic objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iff", kws,
                                         &topics, &future,
                                         &options.validate_only,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of NewTopic objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, "CreateTopics", &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Look up the NewTopic class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        // FIXME

        /*
         * Parse the list of NewTopics and convert to corresponding C types.
         */
        c_objs = alloca(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewTopic *newt = (NewTopic *)PyList_GET_ITEM(topics, i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newt,
                                        (PyObject *)&NewTopicType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected list of NewTopic objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewTopic_new(newt->topic,
                                                   newt->num_partitions,
                                                   newt->replication_factor,
                                                   errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_TypeError,
                                     "Invalid NewTopic(%s): %s",
                                     newt->topic, errstr);
                        goto err;
                }

                if (newt->replica_assignment) {
                        if (newt->replication_factor != -1) {
                                PyErr_SetString(PyExc_TypeError,
                                                "replication_factor and "
                                                "replica_assignment are "
                                                "mutually exclusive");
                                goto err;
                        }

                        if (!Admin_set_replica_assignment(
                                    "CreateTopics", (void *)newt,
                                    newt->replica_assignment,
                                    newt->num_partitions, newt->num_partitions,
                                    "num_partitions"))
                                goto err; /* Exception raised by set_repl.. */
                }
        }


        /* Use librdkafka's internal main thread queue to dispatch
         * Admin_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_internal(self->rk);

        /*
         * Call CreateTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreateTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_NewTopic_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_queue_destroy(rkqu); /* drop our reference from get_internal */

        /* Increase refcount for the return value since
         * it is currently a borrowed reference. */
        Py_INCREF(future);
        return future;

 err:
        rd_kafka_NewTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief delete_topics
 */
static PyObject *Admin_delete_topics (Handle *self, PyObject *args,
                                      PyObject *kwargs) {
        PyObject *topics = NULL, *future;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        rd_kafka_DeleteTopic_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of strings. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!O|ff", kws,
                                         (PyObject *)&PyList_Type, &topics,
                                         &future,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of topic strings");
                return NULL;
        }

        c_options = Admin_options_to_c(self, "DeleteTopics", &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets opaque to the future object, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Look up the NewTopic class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        // FIXME

        /*
         * Parse the list of strings and convert to corresponding C types.
         */
        c_objs = alloca(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                PyObject *topic = PyList_GET_ITEM(topics, i);
                PyObject *utopic;
                PyObject *uotopic = NULL;

                if (!PyUnicode_Check(topic) ||
                    !(utopic = cfl_PyObject_Unistr(topic))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected list of topic strings");
                        goto err;
                }

                c_objs[i] = rd_kafka_DeleteTopic_new(
                        cfl_PyUnistr_AsUTF8(utopic, &uotopic));

                Py_XDECREF(utopic);
                Py_XDECREF(uotopic);
        }


        /* Use librdkafka's internal main thread queue to dispatch
         * Admin_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_internal(self->rk);

        /*
         * Call DeleteTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_DeleteTopic_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_queue_destroy(rkqu); /* drop our reference from get_internal */

        /* Increase refcount for the return value since
         * it is currently a borrowed reference. */
        Py_INCREF(future);
        return future;

 err:
        rd_kafka_DeleteTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief create_partitions
 */
static PyObject *Admin_create_partitions (Handle *self, PyObject *args,
                                          PyObject *kwargs) {
        PyObject *topics = NULL, *future;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        rd_kafka_NewPartitions_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewPartitions_t objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iff", kws,
                                         &topics, &future,
                                         &options.validate_only,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of NewPartitions objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, "CreatePartitions", &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Look up the NewTopic class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        // FIXME

        /*
         * Parse the list of NewPartitions and convert to corresponding C types.
         */
        c_objs = alloca(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewPartitions *newp = (NewPartitions *)PyList_GET_ITEM(topics, i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newp,
                                        (PyObject *)&NewPartitionsType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected list of NewPartitions objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewPartitions_new(newp->topic,
                                                       newp->new_total_count,
                                                       errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_TypeError,
                                     "Invalid NewPartitions(%s): %s",
                                     newp->topic, errstr);
                        goto err;
                }

                if (newp->replica_assignment &&
                    !Admin_set_replica_assignment(
                            "CreatePartitions", (void *)newp,
                            newp->replica_assignment,
                            1, newp->new_total_count,
                            "new_total_count - "
                            "existing partition count"))
                        goto err; /* Exception raised by to_c() */
        }


        /* Use librdkafka's internal main thread queue to dispatch
         * Admin_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_internal(self->rk);

        /*
         * Call CreatePartitions
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreatePartitions(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_NewPartitions_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_queue_destroy(rkqu); /* drop our reference from get_internal */

        /* Increase refcount for the return value since
         * it is currently a borrowed reference. */
        Py_INCREF(future);
        return future;

 err:
        rd_kafka_NewPartitions_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief describe_configs
 */
static PyObject *Admin_describe_configs (Handle *self, PyObject *args,
                                         PyObject *kwargs) {
        PyObject *resources, *future;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "request_timeout",
                               "broker",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        PyObject *ConfigResource_type;
        int cnt, i;
        rd_kafka_ConfigResource_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewPartitions_t objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|fi", kws,
                                         &resources, &future,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, "DescribeConfigs",
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by find() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = alloca(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_TypeError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);
        }


        /* Use librdkafka's internal main thread queue to dispatch
         * Admin_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_internal(self->rk);

        /*
         * Call DescribeConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeConfigs(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_ConfigResource_destroy_array(c_objs, cnt);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_queue_destroy(rkqu); /* drop our reference from get_internal */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        /* Increase refcount for the return value since
         * it is currently a borrowed reference. */
        Py_INCREF(future);
        return future;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}




/**
 * @brief Translate a dict to ConfigResource {set,add,delete}_config() calls.
 *
 * @returns 1 on success or 0 if an exception was raised.
 */
static int
Admin_ConfigEntry_dict_to_c (rd_kafka_ConfigResource_t *c_res,
                             PyObject *dict, const char *op_name) {
        Py_ssize_t pos = 0;
        PyObject *ko, *vo;

        while (PyDict_Next(dict, &pos, &ko, &vo)) {
                PyObject *ks, *ks8;
                const char *k;
                const char *v;
                rd_kafka_resp_err_t err;

                if (!(ks = cfl_PyObject_Unistr(ko))) {
                        PyErr_Format(PyExc_TypeError,
                                     "expected %s config name to be unicode "
                                     "string", op_name);
                        return 0;
                }

                k = cfl_PyUnistr_AsUTF8(ks, &ks8);

                if (!strcmp(op_name, "del_config")) {
                        err = rd_kafka_ConfigResource_delete_config(c_res, k);
                } else {
                        PyObject *vs = NULL, *vs8 = NULL;
                        if (!PyUnicode_Check(vo) ||
                            !(vs = cfl_PyObject_Unistr(vo)) ||
                            !(v = cfl_PyUnistr_AsUTF8(vs, &vs8))) {
                                PyErr_Format(PyExc_TypeError,
                                             "expect %s config value fo %s "
                                             "to be unicode string",
                                             op_name, k);
                                Py_XDECREF(vs);
                                Py_XDECREF(vs8);
                                Py_DECREF(ks);
                                Py_XDECREF(ks8);
                                return 0;
                        }

                        if (!strcmp(op_name, "add_config"))
                                err = rd_kafka_ConfigResource_add_config(
                                        c_res, k, v);
                        else if (!strcmp(op_name, "set_config"))
                                err = rd_kafka_ConfigResource_set_config(
                                        c_res, k, v);
                        else
                                err = RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;

                        if (err) {
                                PyErr_Format(PyExc_ValueError,
                                             "%s config %s failed: %s",
                                             op_name, k, rd_kafka_err2str(err));
                                Py_XDECREF(vs);
                                Py_XDECREF(vs8);
                                Py_DECREF(ks);
                                Py_XDECREF(ks8);
                                return 0;
                        }

                        Py_XDECREF(vs);
                        Py_XDECREF(vs8);
                }
                Py_DECREF(ks);
                Py_XDECREF(ks8);
        }

        return 1;
}


/**
 * @brief alter_configs
 */
static PyObject *Admin_alter_configs (Handle *self, PyObject *args,
                                         PyObject *kwargs) {
        PyObject *resources, *future;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "request_timeout",
                               "broker",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        PyObject *ConfigResource_type;
        int cnt, i;
        rd_kafka_ConfigResource_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewPartitions_t objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|fi", kws,
                                         &resources, &future,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, "AlterConfigs",
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by find() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = alloca(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;
                PyObject *dict;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_TypeError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);

                /*
                 * Translate and apply config entries in the various dicts.
                 */
                if (!cfl_PyObject_GetAttr(res, "set_config_dict", &dict,
                                          &PyDict_Type, 1))
                        goto err;
                if (!Admin_ConfigEntry_dict_to_c(c_objs[i], dict,
                                                 "set_config")) {
                        Py_DECREF(dict);
                        goto err;
                }
                Py_DECREF(dict);

                if (!cfl_PyObject_GetAttr(res, "add_config_dict", &dict,
                                          &PyDict_Type, 1))
                        goto err;
                if (!Admin_ConfigEntry_dict_to_c(c_objs[i], dict,
                                                 "add_config")) {
                        Py_DECREF(dict);
                        goto err;
                }
                Py_DECREF(dict);

                if (!cfl_PyObject_GetAttr(res, "del_config_dict", &dict,
                                          &PyDict_Type, 1))
                        goto err;
                if (!Admin_ConfigEntry_dict_to_c(c_objs[i], dict,
                                                 "del_config")) {
                        Py_DECREF(dict);
                        goto err;
                }
                Py_DECREF(dict);
        }


        /* Use librdkafka's internal main thread queue to dispatch
         * Admin_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_internal(self->rk);

        /*
         * Call AlterConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_AlterConfigs(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_ConfigResource_destroy_array(c_objs, cnt);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_queue_destroy(rkqu); /* drop our reference from get_internal */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        /* Increase refcount for the return value since
         * it is currently a borrowed reference. */
        Py_INCREF(future);
        return future;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}



/**
 * @brief Call rd_kafka_poll() and keep track of crashing callbacks.
 * @returns -1 if callback crashed (or poll() failed), else the number
 * of events served.
 */
static int Admin_poll0 (Handle *self, int tmout) {
        int r;
        CallState cs;

        CallState_begin(self, &cs);

        r = rd_kafka_poll(self->rk, tmout);

        if (!CallState_end(self, &cs)) {
                return -1;
        }

        return r;
}


static PyObject *Admin_poll (Handle *self, PyObject *args,
                             PyObject *kwargs) {
        double tmout;
        int r;
        static char *kws[] = { "timeout", NULL };

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "d", kws, &tmout))
                return NULL;

        r = Admin_poll0(self, (int)(tmout * 1000));
        if (r == -1)
                return NULL;

        return PyLong_FromLong(r);
}



static PyMethodDef Admin_methods[] = {
        { "create_topics", (PyCFunction)Admin_create_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_topics(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
          "  Create new topics.\n"
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
          "  :param int partition: Partition to produce to, elses uses the "
          "configured partitioner.\n"
          "  :param func on_delivery(err,msg): Delivery report callback to call "
          "(from :py:func:`poll()` or :py:func:`flush()`) on successful or "
          "failed delivery\n"
          "  :param int timestamp: Message timestamp (CreateTime) in microseconds since epoch UTC (requires librdkafka >= v0.9.4, api.version.request=true, and broker >= 0.10.0.0). Default value is current time.\n"
          "\n"
          "  :rtype: None\n"
          "  :raises BufferError: if the internal producer message queue is "
          "full (``queue.buffering.max.messages`` exceeded)\n"
          "  :raises KafkaException: for other errors, see exception code\n"
          "  :raises NotImplementedError: if timestamp is specified without underlying library support.\n"
          "\n"
        },

        { "delete_topics", (PyCFunction)Admin_delete_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: delete_topics(topics, future, [request_timeout, operation_timeout])\n"
          "\n"
        },

        { "create_partitions", (PyCFunction)Admin_create_partitions,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_partitions(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
        },

        { "describe_configs", (PyCFunction)Admin_describe_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: describe_configs(resources, future, [request_timeout, broker])\n"
          "\n"
        },

        { "alter_configs", (PyCFunction)Admin_alter_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: alter_configs(resources, future, [request_timeout, broker])\n"
          "\n"
        },


        { "poll", (PyCFunction)Admin_poll, METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: poll([timeout])\n"
          "\n"
          "  Polls the Admin client for event callbacks, such as error_cb, "
          "stats_cb, etc, if registered.\n"
          "\n"
          "  There is no need to call poll() if no callbacks have been registered.\n"
          "\n"
          "  :param float timeout: Maximum time to block waiting for events. (Seconds)\n"
          "  :returns: Number of events processed (callbacks served)\n"
          "  :rtype: int\n"
          "\n"
        },
        { NULL }
};


static Py_ssize_t Admin__len__ (Handle *self) {
        return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Admin_seq_methods = {
        (lenfunc)Admin__len__ /* sq_length */
};


/**
 * @brief Convert C topic_result_t array to topic-indexed dict.
 */
static PyObject *
Admin_c_topic_result_to_py (const rd_kafka_topic_result_t **c_result,
                            size_t cnt) {
        PyObject *result;
        size_t ti;

        result = PyDict_New();

        for (ti = 0 ; ti < cnt ; ti++) {
                PyDict_SetItemString(
                        result,
                        rd_kafka_topic_result_name(c_result[ti]),
                        KafkaError_new_or_None(
                                rd_kafka_topic_result_error(c_result[ti]),
                                rd_kafka_topic_result_error_string(c_result[ti])));
        }

        return result;
}



/**
 * @brief Convert C ConfigEntry array to dict of py ConfigEntry objects.
 */
static PyObject *
Admin_c_ConfigEntries_to_py (PyObject *ConfigEntry_type,
                             const rd_kafka_ConfigEntry_t **c_configs,
                             size_t config_cnt) {
        PyObject *dict;
        size_t ci;

        dict = PyDict_New();

        for (ci = 0 ; ci < config_cnt ; ci++) {
                PyObject *kwargs = PyDict_New();
                const rd_kafka_ConfigEntry_t *ent = c_configs[ci];
                const rd_kafka_ConfigEntry_t **c_synonyms;
                PyObject *entry, *synonyms;
                size_t synonym_cnt;
                const char *val;

                cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_ConfigEntry_name(ent));
                val = rd_kafka_ConfigEntry_value(ent);
                if (val)
                        cfl_PyDict_SetString(kwargs, "value", val);
                else
                        PyDict_SetItemString(kwargs, "value", Py_None);
                cfl_PyDict_SetInt(kwargs, "source",
                                  (int)rd_kafka_ConfigEntry_source(ent));
                cfl_PyDict_SetInt(kwargs, "is_read_only",
                                  rd_kafka_ConfigEntry_is_read_only(ent));
                cfl_PyDict_SetInt(kwargs, "is_default",
                                  rd_kafka_ConfigEntry_is_default(ent));
                cfl_PyDict_SetInt(kwargs, "is_sensitive",
                                  rd_kafka_ConfigEntry_is_sensitive(ent));
                cfl_PyDict_SetInt(kwargs, "is_synonym",
                                  rd_kafka_ConfigEntry_is_synonym(ent));

                c_synonyms = rd_kafka_ConfigEntry_synonyms(ent,
                                                           &synonym_cnt);
                synonyms = Admin_c_ConfigEntries_to_py(ConfigEntry_type,
                                                       c_synonyms,
                                                       synonym_cnt);
                if (!synonyms) {
                        Py_DECREF(kwargs);
                        Py_DECREF(dict);
                        return NULL;
                }
                PyDict_SetItemString(kwargs, "synonyms", synonyms);

                PyObject *wrap = PyTuple_New(0);
                entry = PyObject_Call(ConfigEntry_type, wrap, kwargs);
                Py_DECREF(kwargs);
                if (!entry) {
                        Py_DECREF(dict);
                        return NULL;
                }

                PyDict_SetItemString(dict, rd_kafka_ConfigEntry_name(ent),
                                     entry);
        }


        return dict;
}


/**
 * @brief Convert C ConfigResource array to dict indexed by ConfigResource
 *        with the value of dict(ConfigEntry).
 */
static PyObject *
Admin_c_ConfigResource_result_to_py (const rd_kafka_ConfigResource_t **c_resources,
                                     size_t cnt) {
        PyObject *result;
        PyObject *ConfigResource_type;
        PyObject *ConfigEntry_type;
        size_t ri;

        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConfigResource");
        if (!ConfigResource_type)
                return NULL;

        ConfigEntry_type = cfl_PyObject_lookup("confluent_kafka",
                                               "ConfigEntry");
        if (!ConfigEntry_type) {
                Py_DECREF(ConfigResource_type);
                return NULL;
        }

        result = PyDict_New();

        for (ri = 0 ; ri < cnt ; ri++) {
                const rd_kafka_ConfigResource_t *c_res = c_resources[ri];
                const rd_kafka_ConfigEntry_t **c_configs;
                PyObject *args;
                PyObject *key;
                PyObject *configs, *error;
                size_t config_cnt;

                c_configs = rd_kafka_ConfigResource_configs(c_res, &config_cnt);
                configs = Admin_c_ConfigEntries_to_py(ConfigEntry_type,
                                                      c_configs, config_cnt);
                if (!configs)
                        goto err;

                error = KafkaError_new_or_None(
                        rd_kafka_ConfigResource_error(c_res),
                        rd_kafka_ConfigResource_error_string(c_res));


                args = Py_BuildValue("isOO",
                                     (int)rd_kafka_ConfigResource_type(c_res),
                                     rd_kafka_ConfigResource_name(c_res),
                                     configs,
                                     error);

                key = PyObject_CallObject(ConfigResource_type, args);
                Py_DECREF(args);
                if (!key) {
                        Py_DECREF(configs);
                        goto err;
                }

                PyObject *wrap = PyDict_New();
                PyDict_SetItemString(wrap, "error", error);
                PyDict_SetItemString(wrap, "config", configs);
                PyDict_SetItem(result, key, wrap);
                Py_DECREF(wrap);
        }
        return result;

 err:
        Py_DECREF(ConfigResource_type);
        Py_DECREF(ConfigEntry_type);
        Py_DECREF(result);
        return NULL;
}


/**
 * @brief Event callback triggered from internal librdkafka thread
 *        when Admin API results are ready.
 *
 *        The rkev opaque (not \p opaque) is the future PyObject
 *        which we'll set the result on.
 *
 * @locality internal rdkafka thread
 */
static void Admin_event_cb (rd_kafka_t *rk, rd_kafka_event_t *rkev,
                            void *opaque) {
        PyObject *future = (PyObject *)rd_kafka_event_opaque(rkev);
        const rd_kafka_topic_result_t **c_topic_res;
        size_t c_topic_res_cnt;
        PyGILState_STATE gstate;
        PyObject *error, *method, *ret;
        PyObject *result = NULL;

        /* Acquire GIL */
        gstate = PyGILState_Ensure();

        /* Generic request-level error handling. */
        error = KafkaError_new_or_None(rd_kafka_event_error(rkev),
                                       rd_kafka_event_error_string(rkev));
        if (error != Py_None)
                goto raise;

        switch (rd_kafka_event_type(rkev))
        {
        case RD_KAFKA_EVENT_CREATETOPICS_RESULT:
        {
                const rd_kafka_CreateTopics_result_t *c_res;

                c_res = rd_kafka_event_CreateTopics_result(rkev);

                c_topic_res = rd_kafka_CreateTopics_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DELETETOPICS_RESULT:
        {
                const rd_kafka_DeleteTopics_result_t *c_res;

                c_res = rd_kafka_event_DeleteTopics_result(rkev);

                c_topic_res = rd_kafka_DeleteTopics_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT:
        {
                const rd_kafka_CreatePartitions_result_t *c_res;

                c_res = rd_kafka_event_CreatePartitions_result(rkev);

                c_topic_res = rd_kafka_CreatePartitions_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_DescribeConfigs_result_resources(
                        rd_kafka_event_DescribeConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(c_resources,
                                                             resource_cnt);
                break;
        }

        case RD_KAFKA_EVENT_ALTERCONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_AlterConfigs_result_resources(
                        rd_kafka_event_AlterConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(c_resources,
                                                             resource_cnt);
                break;
        }

        default:
                error = KafkaError_new0(RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                                        "Unsupported event type %s",
                                        rd_kafka_event_name(rkev));
                goto raise;
        }

        if (!result) {
                if (!PyErr_Occurred()) {
                        error = KafkaError_new0(RD_KAFKA_RESP_ERR__INVALID_ARG,
                                                "BUG: Event %s handling failed "
                                                "but no exception raised",
                                                rd_kafka_event_name(rkev));
                } else {
                        /* Extract the exception type and message
                         * and pass it as an error to raise and subsequently
                         * the future.
                         * We loose the backtrace here unfortunately, so
                         * these errors are a bit cryptic. */
                        PyObject *exctype, *trace, *reraise, *excargs;

                        /* Fetch currently raised exception */
                        PyErr_Fetch(&exctype, &error, &trace);

                        /* Create a new exception using the original exception's
                         * type and message. */
                        excargs = PyTuple_New(1);
                        PyTuple_SET_ITEM(excargs, 0, error);
                        reraise = ((PyTypeObject *)exctype)->tp_new(
                                (PyTypeObject *)exctype, NULL, NULL);
                        reraise->ob_type->tp_init(reraise, excargs, NULL);
                        Py_DECREF(excargs);
                        Py_XDECREF(exctype);
                        Py_XDECREF(error);
                        Py_XDECREF(trace);

                        /* error is handled by the raise: goto label below. */
                        error = reraise;

                        /* Clear the current exception. */
                        PyErr_Clear();
                }
                goto raise;
        }

        /*
         * Call future.set_result()
         */
        method = cfl_PyUnistr(_FromString("set_result"));

        ret = PyObject_CallMethodObjArgs(future, method, result, NULL);
        Py_XDECREF(ret);
        Py_XDECREF(result);
        Py_DECREF(future);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);

        return;

 raise:
        /*
         * Call future.set_exception(error)
         */
        method = cfl_PyUnistr(_FromString("set_exception"));
        ret = PyObject_CallMethodObjArgs(future, method, error, NULL);
        Py_XDECREF(ret);
        Py_DECREF(error);

        Py_DECREF(future);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);
}


static int Admin_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Admin already __init__:ialized");
                return -1;
        }

        self->type = PY_RD_KAFKA_ADMIN;

        if (!(conf = common_conf_setup(PY_RD_KAFKA_ADMIN, self,
                                       args, kwargs)))
                return -1;

        rd_kafka_conf_set_event_cb(conf, Admin_event_cb);

        /* There is no dedicated ADMIN client type in librdkafka, the Admin
         * API can use either PRODUCER or CONSUMER.
         * We choose PRODUCER since it is more lightweight than a
         * CONSUMER instance. */
        self->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create admin client: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Forward log messages to poll queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        return 0;
}


static PyObject *Admin_new (PyTypeObject *type, PyObject *args,
                            PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}



PyTypeObject AdminType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "cimpl.AdminClientImpl",   /*tp_name*/
        sizeof(Handle),            /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor)Admin_dealloc, /*tp_dealloc*/
        0,                         /*tp_print*/
        0,                         /*tp_getattr*/
        0,                         /*tp_setattr*/
        0,                         /*tp_compare*/
        0,                         /*tp_repr*/
        0,                         /*tp_as_number*/
        &Admin_seq_methods,        /*tp_as_sequence*/
        0,                         /*tp_as_mapping*/
        0,                         /*tp_hash */
        0,                         /*tp_call*/
        0,                         /*tp_str*/
        0,                         /*tp_getattro*/
        0,                         /*tp_setattro*/
        0,                         /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "Kafka Admin Client\n"
        "\n"
        ".. py:function:: Admin(**kwargs)\n"
        "\n"
        "  Create new AdminClient instance using provided configuration dict.\n"
        "\n"
        "\n"
        ".. py:function:: len()\n"
        "\n"
        "  :returns: Number Kafka protocol requests waiting to be delivered to, or returned from, broker.\n"
        "  :rtype: int\n"
        "\n", /*tp_doc*/
        (traverseproc)Admin_traverse, /* tp_traverse */
        (inquiry)Admin_clear,      /* tp_clear */
        0,                         /* tp_richcompare */
        0,                         /* tp_weaklistoffset */
        0,                         /* tp_iter */
        0,                         /* tp_iternext */
        Admin_methods,             /* tp_methods */
        0,                         /* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        Admin_init,                /* tp_init */
        0,                         /* tp_alloc */
        Admin_new                  /* tp_new */
};




