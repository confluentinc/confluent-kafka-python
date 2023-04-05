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

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Admin_clear(self);

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
#define Admin_options_def_ptr   (NULL)
#define Admin_options_def_cnt   (0)

struct Admin_options {
        int   validate_only;                            /* needs special bool parsing */
        float request_timeout;                          /* parser: f */
        float operation_timeout;                        /* parser: f */
        int   broker;                                   /* parser: i */
        int require_stable_offsets;                     /* needs special bool parsing */
        rd_kafka_consumer_group_state_t* states;
        int states_cnt;
};

/**@brief "unset" value initializers for Admin_options
 * Make sure this is kept up to date with Admin_options above. */
#define Admin_options_INITIALIZER {              \
                Admin_options_def_int,           \
                Admin_options_def_float,         \
                Admin_options_def_float,         \
                Admin_options_def_int,           \
                Admin_options_def_int,           \
                Admin_options_def_ptr,           \
                Admin_options_def_cnt,           \
        }

#define Admin_options_is_set_int(v) ((v) != Admin_options_def_int)
#define Admin_options_is_set_float(v) Admin_options_is_set_int((int)(v))
#define Admin_options_is_set_ptr(v) ((v) != NULL)


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
Admin_options_to_c (Handle *self, rd_kafka_admin_op_t for_api,
                    const struct Admin_options *options,
                    PyObject *future) {
        rd_kafka_AdminOptions_t *c_options;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *err_obj = NULL;
        char errstr[512];

        c_options = rd_kafka_AdminOptions_new(self->rk, for_api);
        if (!c_options) {
                PyErr_Format(PyExc_RuntimeError,
                             "This Admin API method "
                             "is unsupported by librdkafka %s",
                             rd_kafka_version_str());
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

        if (Admin_options_is_set_int(options->require_stable_offsets) &&
            (err_obj = rd_kafka_AdminOptions_set_require_stable_offsets(
                    c_options, options->require_stable_offsets))) {
                strcpy(errstr, rd_kafka_error_string(err_obj));
                goto err;
        }

        if (Admin_options_is_set_ptr(options->states) &&
            (err_obj = rd_kafka_AdminOptions_set_match_consumer_group_states(
                c_options, options->states, options->states_cnt))) {
                strcpy(errstr, rd_kafka_error_string(err_obj));
                goto err;
        }

        return c_options;

 err:
        if (c_options) rd_kafka_AdminOptions_destroy(c_options);
        PyErr_Format(PyExc_ValueError, "%s", errstr);
        if(err_obj) {
                rd_kafka_error_destroy(err_obj);
        }
        return NULL;
}


/**
 * @brief Convert py AclBinding to C
 */
static rd_kafka_AclBinding_t *
Admin_py_to_c_AclBinding (const PyObject *py_obj_arg,
                        char *errstr,
                        size_t errstr_size) {
        int restype, resource_pattern_type, operation, permission_type;
        char *resname = NULL, *principal = NULL, *host = NULL;
        rd_kafka_AclBinding_t *ret = NULL;

        PyObject *py_obj = (PyObject *) py_obj_arg;
        if(cfl_PyObject_GetInt(py_obj, "restype_int", &restype, 0, 1)
            && cfl_PyObject_GetString(py_obj, "name", &resname, NULL, 1, 0)
            && cfl_PyObject_GetInt(py_obj, "resource_pattern_type_int", &resource_pattern_type, 0, 1)
            && cfl_PyObject_GetString(py_obj, "principal", &principal, NULL, 1, 0)
            && cfl_PyObject_GetString(py_obj, "host", &host, NULL, 1, 0)
            && cfl_PyObject_GetInt(py_obj, "operation_int", &operation, 0, 1)
            && cfl_PyObject_GetInt(py_obj, "permission_type_int", &permission_type, 0, 1)) {
                    ret = rd_kafka_AclBinding_new(restype, resname, \
                        resource_pattern_type, principal, host, \
                        operation, permission_type, errstr, errstr_size);
        }
        if (resname) free(resname);
        if (principal) free(principal);
        if (host) free(host);
        return ret;
}

/**
 * @brief Convert py AclBindingFilter to C
 */
static rd_kafka_AclBindingFilter_t*
Admin_py_to_c_AclBindingFilter (const PyObject *py_obj_arg,
                        char *errstr,
                        size_t errstr_size) {
        int restype, resource_pattern_type, operation, permission_type;
        char *resname = NULL, *principal = NULL, *host = NULL;
        PyObject *py_obj = (PyObject *) py_obj_arg;
        rd_kafka_AclBindingFilter_t* ret = NULL;

        if(cfl_PyObject_GetInt(py_obj, "restype_int", &restype, 0, 1)
            && cfl_PyObject_GetString(py_obj, "name", &resname, NULL, 1, 1)
            && cfl_PyObject_GetInt(py_obj, "resource_pattern_type_int", &resource_pattern_type, 0, 1)
            && cfl_PyObject_GetString(py_obj, "principal", &principal, NULL, 1, 1)
            && cfl_PyObject_GetString(py_obj, "host", &host, NULL, 1, 1)
            && cfl_PyObject_GetInt(py_obj, "operation_int", &operation, 0, 1)
            && cfl_PyObject_GetInt(py_obj, "permission_type_int", &permission_type, 0, 1)) {
                    ret = rd_kafka_AclBindingFilter_new(restype, resname, \
                        resource_pattern_type, principal, host, \
                        operation, permission_type, errstr, errstr_size);
        }
        if (resname) free(resname);
        if (principal) free(principal);
        if (host) free(host);
        return ret;
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
                PyErr_Format(PyExc_ValueError,
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
                                PyExc_ValueError,
                                "replica_assignment must be "
                                "a list of int lists with an "
                                "outer size of %s", err_count_desc);
                        return 0;
                }

                c_replicas = malloc(sizeof(*c_replicas) *
                                    replica_cnt);

                for (ri = 0 ; ri < replica_cnt ; ri++) {
                        PyObject *replica =
                                PyList_GET_ITEM(replicas, ri);

                        if (!cfl_PyInt_Check(replica)) {
                                PyErr_Format(
                                        PyExc_ValueError,
                                        "replica_assignment must be "
                                        "a list of int lists with an "
                                        "outer size of %s", err_count_desc);
                                free(c_replicas);
                                return 0;
                        }

                        c_replicas[ri] = (int32_t)cfl_PyInt_AsInt(replica);

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

                free(c_replicas);

                if (err) {
                        PyErr_SetString(
                                PyExc_ValueError, errstr);
                        return 0;
                }
        }

        return 1;
}

/**
 * @brief Translate a dict to ConfigResource set_config() calls,
 *        or to NewTopic_add_config() calls.
 *
 *
 * @returns 1 on success or 0 if an exception was raised.
 */
static int
Admin_config_dict_to_c (void *c_obj, PyObject *dict, const char *op_name) {
        Py_ssize_t pos = 0;
        PyObject *ko, *vo;

        while (PyDict_Next(dict, &pos, &ko, &vo)) {
                PyObject *ks, *ks8;
                PyObject *vs = NULL, *vs8 = NULL;
                const char *k;
                const char *v;
                rd_kafka_resp_err_t err;

                if (!(ks = cfl_PyObject_Unistr(ko))) {
                        PyErr_Format(PyExc_ValueError,
                                     "expected %s config name to be unicode "
                                     "string", op_name);
                        return 0;
                }

                k = cfl_PyUnistr_AsUTF8(ks, &ks8);

                if (!(vs = cfl_PyObject_Unistr(vo)) ||
                    !(v = cfl_PyUnistr_AsUTF8(vs, &vs8))) {
                        PyErr_Format(PyExc_ValueError,
                                     "expect %s config value for %s "
                                     "to be unicode string",
                                     op_name, k);
                        Py_XDECREF(vs);
                        Py_XDECREF(vs8);
                        Py_DECREF(ks);
                        Py_XDECREF(ks8);
                        return 0;
                }

                if (!strcmp(op_name, "set_config"))
                        err = rd_kafka_ConfigResource_set_config(
                                (rd_kafka_ConfigResource_t *)c_obj,
                                k, v);
                else if (!strcmp(op_name, "newtopic_set_config"))
                        err = rd_kafka_NewTopic_set_config(
                                (rd_kafka_NewTopic_t *)c_obj, k, v);
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
                Py_DECREF(ks);
                Py_XDECREF(ks8);
        }

        return 1;
}


/**
 * @brief create_topics
 */
static PyObject *Admin_create_topics (Handle *self, PyObject *args,
                                      PyObject *kwargs) {
        PyObject *topics = NULL, *future, *validate_only_obj = NULL;
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
        int topic_partition_count;
        rd_kafka_NewTopic_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewTopic objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Off", kws,
                                         &topics, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of NewTopic objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATETOPICS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of NewTopics and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewTopic *newt = (NewTopic *)PyList_GET_ITEM(topics, i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newt,
                                        (PyObject *)&NewTopicType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of NewTopic objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewTopic_new(newt->topic,
                                                   newt->num_partitions,
                                                   newt->replication_factor,
                                                   errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid NewTopic(%s): %s",
                                     newt->topic, errstr);
                        goto err;
                }

                if (newt->replica_assignment) {
                        if (newt->replication_factor != -1) {
                                PyErr_SetString(PyExc_ValueError,
                                                "replication_factor and "
                                                "replica_assignment are "
                                                "mutually exclusive");
                                i++;
                                goto err;
                        }

                        if (newt->num_partitions == -1) {
                                topic_partition_count = PyList_Size(newt->replica_assignment);
                        } else {
                                topic_partition_count = newt->num_partitions;
                        }
                        if (!Admin_set_replica_assignment(
                                    "CreateTopics", (void *)c_objs[i],
                                    newt->replica_assignment,
                                    topic_partition_count,
                                    topic_partition_count, 
                                    "num_partitions")) {
                                i++;
                                goto err;
                        }
                }

                if (newt->config) {
                        if (!Admin_config_dict_to_c((void *)c_objs[i],
                                                    newt->config,
                                                    "newtopic_set_config")) {
                                i++;
                                goto err;
                        }
                }
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call CreateTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the background_event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreateTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_NewTopic_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_NewTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
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
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of topic strings");
                return NULL;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETETOPICS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets opaque to the future object, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of strings and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                PyObject *topic = PyList_GET_ITEM(topics, i);
                PyObject *utopic;
                PyObject *uotopic = NULL;

                if (topic == Py_None ||
                    !(utopic = cfl_PyObject_Unistr(topic))) {
                        PyErr_Format(PyExc_ValueError,
                                     "Expected list of topic strings, "
                                     "not %s",
                                     ((PyTypeObject *)PyObject_Type(topic))->
                                     tp_name);
                        goto err;
                }

                c_objs[i] = rd_kafka_DeleteTopic_new(
                        cfl_PyUnistr_AsUTF8(utopic, &uotopic));

                Py_XDECREF(utopic);
                Py_XDECREF(uotopic);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_DeleteTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_DeleteTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief create_partitions
 */
static PyObject *Admin_create_partitions (Handle *self, PyObject *args,
                                          PyObject *kwargs) {
        PyObject *topics = NULL, *future, *validate_only_obj = NULL;
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
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Off", kws,
                                         &topics, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of "
                                "NewPartitions objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of NewPartitions and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewPartitions *newp = (NewPartitions *)PyList_GET_ITEM(topics,
                                                                       i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newp,
                                        (PyObject *)&NewPartitionsType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "NewPartitions objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewPartitions_new(newp->topic,
                                                       newp->new_total_count,
                                                       errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid NewPartitions(%s): %s",
                                     newp->topic, errstr);
                        goto err;
                }

                if (newp->replica_assignment &&
                    !Admin_set_replica_assignment(
                            "CreatePartitions", (void *)c_objs[i],
                            newp->replica_assignment,
                            1, newp->new_total_count,
                            "new_total_count - "
                            "existing partition count")) {
                        i++;
                        goto err; /* Exception raised by set_..() */
                }
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

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
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_NewPartitions_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
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
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by lookup() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype_int", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

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
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        Py_RETURN_NONE;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}




/**
 * @brief alter_configs
 */
static PyObject *Admin_alter_configs (Handle *self, PyObject *args,
                                         PyObject *kwargs) {
        PyObject *resources, *future;
        PyObject *validate_only_obj = NULL;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "validate_only",
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
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Ofi", kws,
                                         &resources, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ALTERCONFIGS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
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
        c_objs = malloc(sizeof(*c_objs) * cnt);

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
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype_int", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
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
                                          &PyDict_Type, 1, 0)) {
                        i++;
                        goto err;
                }
                if (!Admin_config_dict_to_c(c_objs[i], dict, "set_config")) {
                        Py_DECREF(dict);
                        i++;
                        goto err;
                }
                Py_DECREF(dict);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

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
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        Py_RETURN_NONE;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief create_acls
 */
static PyObject *Admin_create_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acls_list, *future;
        int cnt, i = 0;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBinding_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBinding_t **c_objs = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acls",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acls_list,
                                         &future,
                                         &options.request_timeout))
                goto err;

        if (!PyList_Check(acls_list) ||
            (cnt = (int)PyList_Size(acls_list)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Expected non-empty list of AclBinding "
                        "objects");
                goto err;
        }


        /* Look up the AclBinding class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBinding_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBinding");
        if (!AclBinding_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of AclBinding and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                int r;
                PyObject *res = PyList_GET_ITEM(acls_list, i);

                r = PyObject_IsInstance(res, AclBinding_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "AclBinding objects");
                        goto err;
                }


                c_objs[i] = Admin_py_to_c_AclBinding(res, errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_SetString(PyExc_ValueError, errstr);
                        goto err;
                }
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call CreateAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreateAcls(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy_array(c_objs, cnt);
        free(c_objs);
        Py_DECREF(AclBinding_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_objs) {
                rd_kafka_AclBinding_destroy_array(c_objs, i);
                free(c_objs);
        }
        if (AclBinding_type) Py_DECREF(AclBinding_type);
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_create_acls_doc[] = PyDoc_STR(
        ".. py:function:: create_acls(acl_bindings, future, [request_timeout])\n"
        "\n"
        "  Create a list of ACL bindings.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.create_acls()\n"
);


/**
 * @brief describe_acls
 */
static PyObject *Admin_describe_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acl_binding_filter, *future;
        int r;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBindingFilter_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBindingFilter_t *c_obj = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acl_binding_filter",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acl_binding_filter,
                                         &future,
                                         &options.request_timeout))
                goto err;


        /* Look up the AclBindingFilter class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBindingFilter_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBindingFilter");
        if (!AclBindingFilter_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * convert the AclBindingFilter to the
         * corresponding C type.
         */
        r = PyObject_IsInstance(acl_binding_filter, AclBindingFilter_type);
        if (r == -1)
                goto err; /* Exception raised by IsInstance() */
        else if (r == 0) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected an "
                                "AclBindingFilter object");
                goto err;
        }

        c_obj = Admin_py_to_c_AclBindingFilter(acl_binding_filter, errstr, sizeof(errstr));
        if (!c_obj) {
                PyErr_SetString(PyExc_ValueError, errstr);
                goto err;
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeAcls(self->rk, c_obj, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy(c_obj);
        Py_DECREF(AclBindingFilter_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);
        Py_RETURN_NONE;
err:
        if(AclBindingFilter_type) Py_DECREF(AclBindingFilter_type);
        if(c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_describe_acls_doc[] = PyDoc_STR(
        ".. py:function:: describe_acls(acl_binding_filter, future, [request_timeout])\n"
        "\n"
        "  Get a list of ACL bindings matching an ACL binding filter.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_acls()\n"
);

/**
 * @brief delete_acls
 */
static PyObject *Admin_delete_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acls_list, *future;
        int cnt, i = 0;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBindingFilter_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBindingFilter_t **c_objs = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acls",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acls_list,
                                         &future,
                                         &options.request_timeout))
                goto err;

        if (!PyList_Check(acls_list) ||
            (cnt = (int)PyList_Size(acls_list)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Expected non-empty list of AclBindingFilter "
                        "objects");
                goto err;
        }


        /* Look up the AclBindingFilter class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBindingFilter_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBindingFilter");
        if (!AclBindingFilter_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of AclBindingFilter and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                int r;
                PyObject *res = PyList_GET_ITEM(acls_list, i);

                r = PyObject_IsInstance(res, AclBindingFilter_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "AclBindingFilter objects");
                        goto err;
                }


                c_objs[i] = Admin_py_to_c_AclBindingFilter(res, errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_SetString(PyExc_ValueError, errstr);
                        goto err;
                }
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteAcls(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy_array(c_objs, cnt);
        free(c_objs);
        Py_DECREF(AclBindingFilter_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_objs) {
                rd_kafka_AclBinding_destroy_array(c_objs, i);
                free(c_objs);
        }
        if(AclBindingFilter_type) Py_DECREF(AclBindingFilter_type);
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_delete_acls_doc[] = PyDoc_STR(
        ".. py:function:: delete_acls(acl_binding_filters, future, [request_timeout])\n"
        "\n"
        "  Deletes ACL bindings matching one or more ACL binding filter.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.delete_acls()\n"
);


/**
 * @brief List consumer groups
 */
PyObject *Admin_list_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *states_int = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        rd_kafka_consumer_group_state_t *c_states = NULL;
        int states_cnt = 0;
        int i = 0;

        static char *kws[] = {"future",
                             /* options */
                             "states_int",
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Of", kws,
                                         &future,
                                         &states_int,
                                         &options.request_timeout)) {
                goto err;
        }

        if(states_int != NULL && states_int != Py_None) {
                if(!PyList_Check(states_int)) {
                        PyErr_SetString(PyExc_ValueError,
                                "states must of type list");
                        goto err;
                }

                states_cnt = (int)PyList_Size(states_int);

                if(states_cnt > 0) {
                        c_states = (rd_kafka_consumer_group_state_t *)
                                        malloc(states_cnt*sizeof(rd_kafka_consumer_group_state_t));
                        for(i = 0 ; i < states_cnt ; i++) {
                                PyObject *state = PyList_GET_ITEM(states_int, i);
                                if(!cfl_PyInt_Check(state)) {
                                        PyErr_SetString(PyExc_ValueError,
                                                "Element of states must be a valid state");
                                        goto err;
                                }
                                c_states[i] = (rd_kafka_consumer_group_state_t) cfl_PyInt_AsInt(state);
                        }
                        options.states = c_states;
                        options.states_cnt = states_cnt;
                }
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_ListConsumerGroups(self->rk, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_states) {
                free(c_states);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if(c_states) {
                free(c_states);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_list_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: list_consumer_groups(future, [states_int], [request_timeout])\n"
        "\n"
        "  List all the consumer groups.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.list_consumer_groups()\n");


/**
 * @brief Describe consumer groups
 */
PyObject *Admin_describe_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *group_ids;
        struct Admin_options options = Admin_options_INITIALIZER;
        const char **c_groups = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        int groups_cnt = 0;
        int i = 0;

        static char *kws[] = {"future",
                             "group_ids",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &group_ids,
                                         &future,
                                         &options.request_timeout)) {
                goto err;
        }

        if (!PyList_Check(group_ids) || (groups_cnt = (int)PyList_Size(group_ids)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of group_ids");
                goto err;
        }

        c_groups = malloc(sizeof(char *) * groups_cnt);

        for (i = 0 ; i < groups_cnt ; i++) {
                PyObject *group = PyList_GET_ITEM(group_ids, i);
                PyObject *ugroup;
                PyObject *uogroup = NULL;

                if (group == Py_None ||
                    !(ugroup = cfl_PyObject_Unistr(group))) {
                        PyErr_Format(PyExc_ValueError,
                                     "Expected list of group strings, "
                                     "not %s",
                                     ((PyTypeObject *)PyObject_Type(group))->
                                     tp_name);
                        goto err;
                }

                c_groups[i] = cfl_PyUnistr_AsUTF8(ugroup, &uogroup);

                Py_XDECREF(ugroup);
                Py_XDECREF(uogroup);
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeConsumerGroups(self->rk, c_groups, groups_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_groups) {
                free(c_groups);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if(c_groups) {
                free(c_groups);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_describe_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: describe_consumer_groups(future, group_ids, [request_timeout])\n"
        "\n"
        "  Describes the provided consumer groups.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_consumer_groups()\n");


/**
 * @brief Delete consumer groups offsets
 */
PyObject *Admin_delete_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *group_ids, *future;
        PyObject *group_id;
        int group_ids_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_DeleteGroup_t **c_delete_group_ids = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        int i;

        static char *kws[] = {"group_ids",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &group_ids,
                                         &future,
                                         &options.request_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETEGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (!PyList_Check(group_ids)) {
                PyErr_SetString(PyExc_ValueError, "Expected 'group_ids' to be a list");
                goto err;
        }

        group_ids_cnt = (int)PyList_Size(group_ids);

        c_delete_group_ids = malloc(sizeof(rd_kafka_DeleteGroup_t *) * group_ids_cnt);
        for(i = 0 ; i < group_ids_cnt ; i++) {
                group_id = PyList_GET_ITEM(group_ids, i);

                PyObject *ks, *ks8;
                const char *group_id_string;
                if (!(ks = cfl_PyObject_Unistr(group_id))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected element of 'group_ids' "
                                        "to be unicode string");
                        goto err;
                }

                group_id_string = cfl_PyUnistr_AsUTF8(ks, &ks8);

                Py_DECREF(ks);
                Py_XDECREF(ks8);

                c_delete_group_ids[i] = rd_kafka_DeleteGroup_new(group_id_string);
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteGroups
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteGroups(self->rk, c_delete_group_ids, group_ids_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_DeleteGroup_destroy_array(c_delete_group_ids, group_ids_cnt);
        free(c_delete_group_ids);
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_delete_group_ids) {
                rd_kafka_DeleteGroup_destroy_array(c_delete_group_ids, i);
                free(c_delete_group_ids);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_delete_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: delete_consumer_groups(request, future, [request_timeout])\n"
        "\n"
        "  Deletes consumer groups provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.delete_consumer_groups()\n");


/**
 * @brief List consumer groups offsets
 */
PyObject *Admin_list_consumer_group_offsets (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *request, *future, *require_stable_obj = NULL;
        int requests_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *ConsumerGroupTopicPartitions_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_ListConsumerGroupOffsets_t **c_obj = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        PyObject *topic_partitions = NULL;
        char *group_id = NULL;

        static char *kws[] = {"request",
                             "future",
                             /* options */
                             "require_stable",
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Of", kws,
                                         &request,
                                         &future,
                                         &require_stable_obj,
                                         &options.request_timeout)) {
                goto err;
        }

        if (require_stable_obj &&
            !cfl_PyBool_get(require_stable_obj, "require_stable",
                            &options.require_stable_offsets))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (PyList_Check(request) &&
            (requests_cnt = (int)PyList_Size(request)) != 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Currently we support listing only 1 consumer groups offset information");
                goto err;
        }

        PyObject *single_request = PyList_GET_ITEM(request, 0);

        /* Look up the ConsumerGroupTopicPartition class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConsumerGroupTopicPartitions_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConsumerGroupTopicPartitions");
        if (!ConsumerGroupTopicPartitions_type) {
                PyErr_SetString(PyExc_ImportError,
                        "Not able to load ConsumerGroupTopicPartitions type");
                goto err;
        }

        if(!PyObject_IsInstance(single_request, ConsumerGroupTopicPartitions_type)) {
                PyErr_SetString(PyExc_ImportError,
                        "Each request should be of ConsumerGroupTopicPartitions type");
                goto err;
        }

        cfl_PyObject_GetString(single_request, "group_id", &group_id, NULL, 1, 0);

        if(group_id == NULL) {
                PyErr_SetString(PyExc_ValueError,
                        "Group name is mandatory for list consumer offset operation");
                goto err;
        }

        cfl_PyObject_GetAttr(single_request, "topic_partitions", &topic_partitions, &PyList_Type, 0, 1);

        if(topic_partitions != Py_None) {
                c_topic_partitions = py_to_c_parts(topic_partitions);
        }

        c_obj = malloc(sizeof(rd_kafka_ListConsumerGroupOffsets_t *) * requests_cnt);
        c_obj[0] = rd_kafka_ListConsumerGroupOffsets_new(group_id, c_topic_partitions);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_ListConsumerGroupOffsets(self->rk, c_obj, requests_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if (c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_ListConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
        free(c_obj);
        free(group_id);
        Py_DECREF(ConsumerGroupTopicPartitions_type); /* from lookup() */
        Py_XDECREF(topic_partitions);
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        if (c_obj) {
                rd_kafka_ListConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
                free(c_obj);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        if(group_id) {
                free(group_id);
        }
        Py_XDECREF(topic_partitions);
        Py_XDECREF(ConsumerGroupTopicPartitions_type);
        return NULL;
}


const char Admin_list_consumer_group_offsets_doc[] = PyDoc_STR(
        ".. py:function:: list_consumer_group_offsets(request, future, [require_stable], [request_timeout])\n"
        "\n"
        "  List offset information for the consumer group and (optional) topic partition provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.list_consumer_group_offsets()\n");


/**
 * @brief Alter consumer groups offsets
 */
PyObject *Admin_alter_consumer_group_offsets (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *request, *future;
        int requests_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *ConsumerGroupTopicPartitions_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AlterConsumerGroupOffsets_t **c_obj = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        PyObject *topic_partitions = NULL;
        char *group_id = NULL;

        static char *kws[] = {"request",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &request,
                                         &future,
                                         &options.request_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (PyList_Check(request) &&
            (requests_cnt = (int)PyList_Size(request)) != 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Currently we support alter consumer groups offset request for 1 group only");
                goto err;
        }

        PyObject *single_request = PyList_GET_ITEM(request, 0);

        /* Look up the ConsumerGroupTopicPartition class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConsumerGroupTopicPartitions_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConsumerGroupTopicPartitions");
        if (!ConsumerGroupTopicPartitions_type) {
                PyErr_SetString(PyExc_ImportError,
                        "Not able to load ConsumerGroupTopicPartitions type");
                goto err;
        }

        if(!PyObject_IsInstance(single_request, ConsumerGroupTopicPartitions_type)) {
                PyErr_SetString(PyExc_ImportError,
                        "Each request should be of ConsumerGroupTopicPartitions type");
                goto err;
        }

        cfl_PyObject_GetString(single_request, "group_id", &group_id, NULL, 1, 0);

        if(group_id == NULL) {
                PyErr_SetString(PyExc_ValueError,
                        "Group name is mandatory for alter consumer offset operation");
                goto err;
        }

        cfl_PyObject_GetAttr(single_request, "topic_partitions", &topic_partitions, &PyList_Type, 0, 1);

        if(topic_partitions != Py_None) {
                c_topic_partitions = py_to_c_parts(topic_partitions);
        }

        c_obj = malloc(sizeof(rd_kafka_AlterConsumerGroupOffsets_t *) * requests_cnt);
        c_obj[0] = rd_kafka_AlterConsumerGroupOffsets_new(group_id, c_topic_partitions);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call AlterConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_AlterConsumerGroupOffsets(self->rk, c_obj, requests_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AlterConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
        free(c_obj);
        free(group_id);
        Py_DECREF(ConsumerGroupTopicPartitions_type); /* from lookup() */
        Py_XDECREF(topic_partitions);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_topic_partition_list_destroy(c_topic_partitions);

        Py_RETURN_NONE;
err:
        if (c_obj) {
                rd_kafka_AlterConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
                free(c_obj);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        if(c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        if(group_id) {
                free(group_id);
        }
        Py_XDECREF(topic_partitions);
        Py_XDECREF(ConsumerGroupTopicPartitions_type);
        return NULL;
}


const char Admin_alter_consumer_group_offsets_doc[] = PyDoc_STR(
        ".. py:function:: alter_consumer_group_offsets(request, future, [request_timeout])\n"
        "\n"
        "  Alter offset for the consumer group and topic partition provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.alter_consumer_group_offsets()\n");


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

        return cfl_PyInt_FromInt(r);
}



static PyMethodDef Admin_methods[] = {
        { "create_topics", (PyCFunction)Admin_create_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_topics(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
          "  Create new topics.\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.create_topics()\n"
        },

        { "delete_topics", (PyCFunction)Admin_delete_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: delete_topics(topics, future, [request_timeout, operation_timeout])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.delete_topics()\n"
        },

        { "create_partitions", (PyCFunction)Admin_create_partitions,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_partitions(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.create_partitions()\n"
        },

        { "describe_configs", (PyCFunction)Admin_describe_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: describe_configs(resources, future, [request_timeout, broker])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.describe_configs()\n"
        },

        { "alter_configs", (PyCFunction)Admin_alter_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: alter_configs(resources, future, [request_timeout, broker])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.alter_configs()\n"
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

        { "list_topics", (PyCFunction)list_topics, METH_VARARGS|METH_KEYWORDS,
          list_topics_doc
        },

        { "list_groups", (PyCFunction)list_groups, METH_VARARGS|METH_KEYWORDS,
          list_groups_doc
        },

        { "list_consumer_groups", (PyCFunction)Admin_list_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_list_consumer_groups_doc
        },

        { "describe_consumer_groups", (PyCFunction)Admin_describe_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_describe_consumer_groups_doc
        },

        { "delete_consumer_groups", (PyCFunction)Admin_delete_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_delete_consumer_groups_doc
        },

        { "list_consumer_group_offsets", (PyCFunction)Admin_list_consumer_group_offsets, METH_VARARGS|METH_KEYWORDS,
          Admin_list_consumer_group_offsets_doc
        },

        { "alter_consumer_group_offsets", (PyCFunction)Admin_alter_consumer_group_offsets, METH_VARARGS|METH_KEYWORDS,
          Admin_alter_consumer_group_offsets_doc
        },

        { "create_acls", (PyCFunction)Admin_create_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_create_acls_doc
        },

        { "describe_acls", (PyCFunction)Admin_describe_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_describe_acls_doc
        },

        { "delete_acls", (PyCFunction)Admin_delete_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_delete_acls_doc
        },

        { "set_sasl_credentials", (PyCFunction)set_sasl_credentials, METH_VARARGS|METH_KEYWORDS,
           set_sasl_credentials_doc
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
        size_t i;

        result = PyDict_New();

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;

                error = KafkaError_new_or_None(
                        rd_kafka_topic_result_error(c_result[i]),
                        rd_kafka_topic_result_error_string(c_result[i]));

                PyDict_SetItemString(
                        result,
                        rd_kafka_topic_result_name(c_result[i]),
                        error);

                Py_DECREF(error);
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
                PyObject *kwargs, *args;
                const rd_kafka_ConfigEntry_t *ent = c_configs[ci];
                const rd_kafka_ConfigEntry_t **c_synonyms;
                PyObject *entry, *synonyms;
                size_t synonym_cnt;
                const char *val;

                kwargs = PyDict_New();

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
                Py_DECREF(synonyms);

                args = PyTuple_New(0);
                entry = PyObject_Call(ConfigEntry_type, args, kwargs);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                if (!entry) {
                        Py_DECREF(dict);
                        return NULL;
                }

                PyDict_SetItemString(dict, rd_kafka_ConfigEntry_name(ent),
                                     entry);
                Py_DECREF(entry);
        }


        return dict;
}


/**
 * @brief Convert C ConfigResource array to dict indexed by ConfigResource
 *        with the value of dict(ConfigEntry).
 *
 * @param ret_configs If true, return configs rather than None.
 */
static PyObject *
Admin_c_ConfigResource_result_to_py (const rd_kafka_ConfigResource_t **c_resources,
                                     size_t cnt,
                                     int ret_configs) {
        PyObject *result;
        PyObject *ConfigResource_type;
        PyObject *ConfigEntry_type;
        size_t ri;

        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type)
                return NULL;

        ConfigEntry_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                               "ConfigEntry");
        if (!ConfigEntry_type) {
                Py_DECREF(ConfigResource_type);
                return NULL;
        }

        result = PyDict_New();

        for (ri = 0 ; ri < cnt ; ri++) {
                const rd_kafka_ConfigResource_t *c_res = c_resources[ri];
                const rd_kafka_ConfigEntry_t **c_configs;
                PyObject *kwargs, *wrap;
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

                kwargs = PyDict_New();
                cfl_PyDict_SetInt(kwargs, "restype",
                                  (int)rd_kafka_ConfigResource_type(c_res));
                cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_ConfigResource_name(c_res));
                PyDict_SetItemString(kwargs, "described_configs", configs);
                PyDict_SetItemString(kwargs, "error", error);
                Py_DECREF(error);

                /* Instantiate ConfigResource */
                wrap = PyTuple_New(0);
                key = PyObject_Call(ConfigResource_type, wrap, kwargs);
                Py_DECREF(wrap);
                Py_DECREF(kwargs);
                if (!key) {
                        Py_DECREF(configs);
                        goto err;
                }

                /* Set result to dict[ConfigResource(..)] = configs | None
                 * depending on ret_configs */
                if (ret_configs)
                        PyDict_SetItem(result, key, configs);
                else
                        PyDict_SetItem(result, key, Py_None);

                Py_DECREF(configs);
                Py_DECREF(key);
        }
        return result;

 err:
        Py_DECREF(ConfigResource_type);
        Py_DECREF(ConfigEntry_type);
        Py_DECREF(result);
        return NULL;
}

/**
 * @brief Convert C AclBinding to py
 */
static PyObject *
Admin_c_AclBinding_to_py (const rd_kafka_AclBinding_t *c_acl_binding) {

        PyObject *args, *kwargs, *AclBinding_type, *acl_binding;

        AclBinding_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                        "AclBinding");
        if (!AclBinding_type) {
                return NULL;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetInt(kwargs, "restype",
                                     rd_kafka_AclBinding_restype(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_AclBinding_name(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "resource_pattern_type",
                                rd_kafka_AclBinding_resource_pattern_type(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "principal",
                                     rd_kafka_AclBinding_principal(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "host",
                                     rd_kafka_AclBinding_host(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "operation",
                                     rd_kafka_AclBinding_operation(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "permission_type",
                                     rd_kafka_AclBinding_permission_type(c_acl_binding));

        args = PyTuple_New(0);
        acl_binding = PyObject_Call(AclBinding_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(AclBinding_type);
        return acl_binding;
}

/**
 * @brief Convert C AclBinding array to py list.
 */
static PyObject *
Admin_c_AclBindings_to_py (const rd_kafka_AclBinding_t **c_acls,
                                          size_t c_acls_cnt) {
        size_t i;
        PyObject *result;
        PyObject *acl_binding;

        result = PyList_New(c_acls_cnt);

        for (i = 0 ; i < c_acls_cnt ; i++) {
                acl_binding = Admin_c_AclBinding_to_py(c_acls[i]);
                if (!acl_binding) {
                        Py_DECREF(result);
                        return NULL;
                }
                PyList_SET_ITEM(result, i, acl_binding);
        }

        return result;
}


/**
 * @brief Convert C acl_result_t array to py list.
 */
static PyObject *
Admin_c_acl_result_to_py (const rd_kafka_acl_result_t **c_result,
                            size_t cnt) {
        PyObject *result;
        size_t i;

        result = PyList_New(cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_acl_result_error(c_result[i]);

                error = KafkaError_new_or_None(
                        rd_kafka_error_code(c_error),
                        rd_kafka_error_string(c_error));

                PyList_SET_ITEM(result, i, error);
        }

        return result;
}

/**
 * @brief Convert C DeleteAcls result response array to py list.
 */
static PyObject *
Admin_c_DeleteAcls_result_responses_to_py (const rd_kafka_DeleteAcls_result_response_t **c_result_responses,
                            size_t cnt) {
        const rd_kafka_AclBinding_t **c_matching_acls;
        size_t c_matching_acls_cnt;
        PyObject *result;
        PyObject *acl_bindings;
        size_t i;

        result = PyList_New(cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_DeleteAcls_result_response_error(c_result_responses[i]);

                if (c_error) {
                        error = KafkaError_new_or_None(
                                rd_kafka_error_code(c_error),
                                rd_kafka_error_string(c_error));
                        PyList_SET_ITEM(result, i, error);
                } else {
                        c_matching_acls = rd_kafka_DeleteAcls_result_response_matching_acls(
                                                                        c_result_responses[i],
                                                                        &c_matching_acls_cnt);
                        acl_bindings = Admin_c_AclBindings_to_py(c_matching_acls,c_matching_acls_cnt);
                        if (!acl_bindings) {
                                Py_DECREF(result);
                                return NULL;
                        }
                        PyList_SET_ITEM(result, i, acl_bindings);
                }
        }

        return result;
}


/**
 * @brief
 *
 */
static PyObject *Admin_c_ListConsumerGroupsResults_to_py(
                        const rd_kafka_ConsumerGroupListing_t **c_valid_responses,
                        size_t valid_cnt,
                        const rd_kafka_error_t **c_errors_responses,
                        size_t errors_cnt) {

        PyObject *result = NULL;
        PyObject *ListConsumerGroupsResult_type = NULL;
        PyObject *ConsumerGroupListing_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *valid_result = NULL;
        PyObject *valid_results = NULL;
        PyObject *error_result = NULL;
        PyObject *error_results = NULL;
        PyObject *py_is_simple_consumer_group = NULL;
        size_t i = 0;
        valid_results = PyList_New(valid_cnt);
        error_results = PyList_New(errors_cnt);
        if(valid_cnt > 0) {
                ConsumerGroupListing_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                                "ConsumerGroupListing");
                if (!ConsumerGroupListing_type) {
                        goto err;
                }
                for(i = 0; i < valid_cnt; i++) {

                        kwargs = PyDict_New();

                        cfl_PyDict_SetString(kwargs,
                                             "group_id",
                                             rd_kafka_ConsumerGroupListing_group_id(c_valid_responses[i]));


                        py_is_simple_consumer_group = PyBool_FromLong(
                                rd_kafka_ConsumerGroupListing_is_simple_consumer_group(c_valid_responses[i]));
                        if(PyDict_SetItemString(kwargs,
                                                "is_simple_consumer_group",
                                                py_is_simple_consumer_group) == -1) {
                                PyErr_Format(PyExc_RuntimeError,
                                             "Not able to set 'is_simple_consumer_group' in ConsumerGroupLising");
                                Py_DECREF(py_is_simple_consumer_group);
                                goto err;
                        }
                        Py_DECREF(py_is_simple_consumer_group);

                        cfl_PyDict_SetInt(kwargs, "state", rd_kafka_ConsumerGroupListing_state(c_valid_responses[i]));

                        args = PyTuple_New(0);

                        valid_result = PyObject_Call(ConsumerGroupListing_type, args, kwargs);
                        PyList_SET_ITEM(valid_results, i, valid_result);

                        Py_DECREF(args);
                        Py_DECREF(kwargs);
                }
                Py_DECREF(ConsumerGroupListing_type);
        }

        if(errors_cnt > 0) {
                for(i = 0; i < errors_cnt; i++) {

                        error_result = KafkaError_new_or_None(
                                rd_kafka_error_code(c_errors_responses[i]),
                                rd_kafka_error_string(c_errors_responses[i]));
                        PyList_SET_ITEM(error_results, i, error_result);

                }
        }

        ListConsumerGroupsResult_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                              "ListConsumerGroupsResult");
        if (!ListConsumerGroupsResult_type) {
                return NULL;
        }
        kwargs = PyDict_New();
        PyDict_SetItemString(kwargs, "valid", valid_results);
        PyDict_SetItemString(kwargs, "errors", error_results);
        args = PyTuple_New(0);
        result = PyObject_Call(ListConsumerGroupsResult_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(valid_results);
        Py_DECREF(error_results);
        Py_DECREF(ListConsumerGroupsResult_type);

        return result;
err:
        Py_XDECREF(ListConsumerGroupsResult_type);
        Py_XDECREF(ConsumerGroupListing_type);
        Py_XDECREF(result);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);

        return NULL;
}

static PyObject *Admin_c_MemberAssignment_to_py(const rd_kafka_MemberAssignment_t *c_assignment) {
        PyObject *MemberAssignment_type = NULL;
        PyObject *assignment = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *topic_partitions = NULL;
        const rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;

        MemberAssignment_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                     "MemberAssignment");
        if (!MemberAssignment_type) {
                goto err;
        }
        c_topic_partitions = rd_kafka_MemberAssignment_partitions(c_assignment);

        topic_partitions = c_parts_to_py(c_topic_partitions);

        kwargs = PyDict_New();

        PyDict_SetItemString(kwargs, "topic_partitions", topic_partitions);

        args = PyTuple_New(0);

        assignment = PyObject_Call(MemberAssignment_type, args, kwargs);

        Py_DECREF(MemberAssignment_type);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(topic_partitions);
        return assignment;

err:
        Py_XDECREF(MemberAssignment_type);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(topic_partitions);
        Py_XDECREF(assignment);
        return NULL;

}

static PyObject *Admin_c_MemberDescription_to_py(const rd_kafka_MemberDescription_t *c_member) {
        PyObject *member = NULL;
        PyObject *MemberDescription_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *assignment = NULL;
        const rd_kafka_MemberAssignment_t *c_assignment;

        MemberDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                     "MemberDescription");
        if (!MemberDescription_type) {
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs,
                             "member_id",
                             rd_kafka_MemberDescription_consumer_id(c_member));

        cfl_PyDict_SetString(kwargs,
                             "client_id",
                             rd_kafka_MemberDescription_client_id(c_member));

        cfl_PyDict_SetString(kwargs,
                             "host",
                             rd_kafka_MemberDescription_host(c_member));

        const char * c_group_instance_id = rd_kafka_MemberDescription_group_instance_id(c_member);
        if(c_group_instance_id) {
                cfl_PyDict_SetString(kwargs, "group_instance_id", c_group_instance_id);
        }

        c_assignment = rd_kafka_MemberDescription_assignment(c_member);
        assignment = Admin_c_MemberAssignment_to_py(c_assignment);
        if (!assignment) {
                goto err;
        }

        PyDict_SetItemString(kwargs, "assignment", assignment);

        args = PyTuple_New(0);

        member = PyObject_Call(MemberDescription_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(MemberDescription_type);
        Py_DECREF(assignment);
        return member;

err:

        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(MemberDescription_type);
        Py_XDECREF(assignment);
        Py_XDECREF(member);
        return NULL;
}

static PyObject *Admin_c_MemberDescriptions_to_py_from_ConsumerGroupDescription(
    const rd_kafka_ConsumerGroupDescription_t *c_consumer_group_description) {
        PyObject *member_description = NULL;
        PyObject *members = NULL;
        size_t c_members_cnt;
        const rd_kafka_MemberDescription_t *c_member;
        size_t i = 0;

        c_members_cnt = rd_kafka_ConsumerGroupDescription_member_count(c_consumer_group_description);
        members = PyList_New(c_members_cnt);
        if(c_members_cnt > 0) {
                for(i = 0; i < c_members_cnt; i++) {

                        c_member = rd_kafka_ConsumerGroupDescription_member(c_consumer_group_description, i);
                        member_description = Admin_c_MemberDescription_to_py(c_member);
                        if(!member_description) {
                                goto err;
                        }
                        PyList_SET_ITEM(members, i, member_description);
                }
        }
        return members;
err:
        Py_XDECREF(members);
        return NULL;
}


static PyObject *Admin_c_ConsumerGroupDescription_to_py(
    const rd_kafka_ConsumerGroupDescription_t *c_consumer_group_description) {
        PyObject *consumer_group_description = NULL;
        PyObject *ConsumerGroupDescription_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *py_is_simple_consumer_group = NULL;
        PyObject *coordinator = NULL;
        PyObject *members = NULL;
        const rd_kafka_Node_t *c_coordinator = NULL;

        ConsumerGroupDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                            "ConsumerGroupDescription");
        if (!ConsumerGroupDescription_type) {
                PyErr_Format(PyExc_TypeError, "Not able to load ConsumerGroupDescrition type");
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs,
                             "group_id",
                             rd_kafka_ConsumerGroupDescription_group_id(c_consumer_group_description));

        cfl_PyDict_SetString(kwargs,
                             "partition_assignor",
                             rd_kafka_ConsumerGroupDescription_partition_assignor(c_consumer_group_description));

        members = Admin_c_MemberDescriptions_to_py_from_ConsumerGroupDescription(c_consumer_group_description);
        if(!members) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "members", members);

        c_coordinator = rd_kafka_ConsumerGroupDescription_coordinator(c_consumer_group_description);
        coordinator = c_Node_to_py(c_coordinator);
        if(!coordinator) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "coordinator", coordinator);

        py_is_simple_consumer_group = PyBool_FromLong(
                rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(c_consumer_group_description));
        if(PyDict_SetItemString(kwargs, "is_simple_consumer_group", py_is_simple_consumer_group) == -1) {
                goto err;
        }

        cfl_PyDict_SetInt(kwargs, "state", rd_kafka_ConsumerGroupDescription_state(c_consumer_group_description));

        args = PyTuple_New(0);

        consumer_group_description = PyObject_Call(ConsumerGroupDescription_type, args, kwargs);

        Py_DECREF(py_is_simple_consumer_group);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(ConsumerGroupDescription_type);
        Py_DECREF(coordinator);
        Py_DECREF(members);
        return consumer_group_description;

err:
        Py_XDECREF(py_is_simple_consumer_group);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(coordinator);
        Py_XDECREF(ConsumerGroupDescription_type);
        Py_XDECREF(members);
        return NULL;

}

static PyObject *Admin_c_DescribeConsumerGroupsResults_to_py(
    const rd_kafka_ConsumerGroupDescription_t **c_result_responses,
    size_t cnt) {
        PyObject *consumer_group_description = NULL;
        PyObject *results = NULL;
        size_t i = 0;
        results = PyList_New(cnt);
        if(cnt > 0) {
                for(i = 0; i < cnt; i++) {
                        PyObject *error;
                        const rd_kafka_error_t *c_error =
                            rd_kafka_ConsumerGroupDescription_error(c_result_responses[i]);

                        if (c_error) {
                                error = KafkaError_new_or_None(
                                        rd_kafka_error_code(c_error),
                                        rd_kafka_error_string(c_error));
                                PyList_SET_ITEM(results, i, error);
                        } else {
                                consumer_group_description =
                                    Admin_c_ConsumerGroupDescription_to_py(c_result_responses[i]);

                                if(!consumer_group_description) {
                                        goto err;
                                }

                                PyList_SET_ITEM(results, i, consumer_group_description);
                        }
                }
        }
        return results;
err:
        Py_XDECREF(results);
        return NULL;
}


/**
 *
 * @brief Convert C delete groups result response to pyobject.
 *
 */
static PyObject *
Admin_c_DeleteGroupResults_to_py (const rd_kafka_group_result_t **c_result_responses,
                                  size_t cnt) {

        PyObject *delete_groups_result = NULL;
        size_t i;

        delete_groups_result = PyList_New(cnt);

        for (i = 0; i < cnt; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_group_result_error(c_result_responses[i]);
                error = KafkaError_new_or_None(
                        rd_kafka_error_code(c_error),
                        rd_kafka_error_string(c_error));
                PyList_SET_ITEM(delete_groups_result, i, error);
        }

        return delete_groups_result;
}


static PyObject * Admin_c_SingleGroupResult_to_py(const rd_kafka_group_result_t *c_group_result_response) {

        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *GroupResult_type = NULL;
        PyObject *group_result = NULL;
        const rd_kafka_topic_partition_list_t *c_topic_partition_offset_list;
        PyObject *topic_partition_offset_list = NULL;

        GroupResult_type = cfl_PyObject_lookup("confluent_kafka",
                                               "ConsumerGroupTopicPartitions");
        if (!GroupResult_type) {
                return NULL;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs, "group_id", rd_kafka_group_result_name(c_group_result_response));

        c_topic_partition_offset_list = rd_kafka_group_result_partitions(c_group_result_response);
        if(c_topic_partition_offset_list) {
                topic_partition_offset_list = c_parts_to_py(c_topic_partition_offset_list);
                PyDict_SetItemString(kwargs, "topic_partitions", topic_partition_offset_list);
        }

        args = PyTuple_New(0);
        group_result = PyObject_Call(GroupResult_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(GroupResult_type);
        Py_XDECREF(topic_partition_offset_list);

        return group_result;
}


/**
 *
 * @brief Convert C group result response to pyobject.
 *
 */
static PyObject *
Admin_c_GroupResults_to_py (const rd_kafka_group_result_t **c_result_responses,
                            size_t cnt) {

        size_t i;
        PyObject *all_groups_result = NULL;
        PyObject *single_group_result = NULL;

        all_groups_result = PyList_New(cnt);

        for (i = 0; i < cnt; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_group_result_error(c_result_responses[i]);

                if (c_error) {
                        error = KafkaError_new_or_None(
                                rd_kafka_error_code(c_error),
                                rd_kafka_error_string(c_error));
                        PyList_SET_ITEM(all_groups_result, i, error);
                } else {
                        single_group_result =
                                Admin_c_SingleGroupResult_to_py(c_result_responses[i]);
                        if (!single_group_result) {
                                Py_XDECREF(all_groups_result);
                                return NULL;
                        }
                        PyList_SET_ITEM(all_groups_result, i, single_group_result);
                }
        }

        return all_groups_result;
}


/**
 * @brief Event callback triggered from librdkafka's background thread
 *        when Admin API results are ready.
 *
 *        The rkev opaque (not \p opaque) is the future PyObject
 *        which we'll set the result on.
 *
 * @locality background rdkafka thread
 */
static void Admin_background_event_cb (rd_kafka_t *rk, rd_kafka_event_t *rkev,
                                       void *opaque) {
        PyObject *future = (PyObject *)rd_kafka_event_opaque(rkev);
        const rd_kafka_topic_result_t **c_topic_res;
        size_t c_topic_res_cnt;
        PyGILState_STATE gstate;
        PyObject *error, *method, *ret;
        PyObject *result = NULL;
        PyObject *exctype = NULL, *exc = NULL, *excargs = NULL;

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
                result = Admin_c_ConfigResource_result_to_py(
                        c_resources,
                        resource_cnt,
                        1/* return configs */);
                break;
        }

        case RD_KAFKA_EVENT_ALTERCONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_AlterConfigs_result_resources(
                        rd_kafka_event_AlterConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(
                        c_resources,
                        resource_cnt,
                        0/* return None instead of (the empty) configs */);
                break;
        }


        case RD_KAFKA_EVENT_CREATEACLS_RESULT:
        {
                const rd_kafka_acl_result_t **c_acl_results;
                size_t c_acl_results_cnt;

                c_acl_results = rd_kafka_CreateAcls_result_acls(
                        rd_kafka_event_CreateAcls_result(rkev),
                        &c_acl_results_cnt
                );
                result = Admin_c_acl_result_to_py(
                        c_acl_results,
                        c_acl_results_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DESCRIBEACLS_RESULT:
        {
                const rd_kafka_DescribeAcls_result_t *c_acl_result;
                const rd_kafka_AclBinding_t **c_acls;
                size_t c_acl_cnt;

                c_acl_result = rd_kafka_event_DescribeAcls_result(rkev);

                c_acls = rd_kafka_DescribeAcls_result_acls(
                        c_acl_result,
                        &c_acl_cnt
                );

                result = Admin_c_AclBindings_to_py(c_acls,
                                                   c_acl_cnt);

                break;
        }


        case RD_KAFKA_EVENT_DELETEACLS_RESULT:
        {
                const rd_kafka_DeleteAcls_result_t *c_acl_result;
                const rd_kafka_DeleteAcls_result_response_t **c_acl_result_responses;
                size_t c_acl_results_cnt;

                c_acl_result = rd_kafka_event_DeleteAcls_result(rkev);

                c_acl_result_responses = rd_kafka_DeleteAcls_result_responses(
                        c_acl_result,
                        &c_acl_results_cnt
                );

                result = Admin_c_DeleteAcls_result_responses_to_py(c_acl_result_responses,
                                                        c_acl_results_cnt);

                break;
        }

        case RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT:
        {
                const  rd_kafka_ListConsumerGroups_result_t *c_list_consumer_groups_res;
                const rd_kafka_ConsumerGroupListing_t **c_list_consumer_groups_valid_responses;
                size_t c_list_consumer_groups_valid_cnt;
                const rd_kafka_error_t **c_list_consumer_groups_errors_responses;
                size_t c_list_consumer_groups_errors_cnt;

                c_list_consumer_groups_res = rd_kafka_event_ListConsumerGroups_result(rkev);

                c_list_consumer_groups_valid_responses =
                        rd_kafka_ListConsumerGroups_result_valid(c_list_consumer_groups_res,
                                                                 &c_list_consumer_groups_valid_cnt);
                c_list_consumer_groups_errors_responses =
                        rd_kafka_ListConsumerGroups_result_errors(c_list_consumer_groups_res,
                                                                  &c_list_consumer_groups_errors_cnt);

                result = Admin_c_ListConsumerGroupsResults_to_py(c_list_consumer_groups_valid_responses,
                                                                 c_list_consumer_groups_valid_cnt,
                                                                 c_list_consumer_groups_errors_responses,
                                                                 c_list_consumer_groups_errors_cnt);

                break;
        }

        case RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT:
        {
                const rd_kafka_DescribeConsumerGroups_result_t *c_describe_consumer_groups_res;
                const rd_kafka_ConsumerGroupDescription_t **c_describe_consumer_groups_res_responses;
                size_t c_describe_consumer_groups_res_cnt;

                c_describe_consumer_groups_res = rd_kafka_event_DescribeConsumerGroups_result(rkev);

                c_describe_consumer_groups_res_responses = rd_kafka_DescribeConsumerGroups_result_groups
                                                           (c_describe_consumer_groups_res,
                                                           &c_describe_consumer_groups_res_cnt);

                result = Admin_c_DescribeConsumerGroupsResults_to_py(c_describe_consumer_groups_res_responses,
                                                                     c_describe_consumer_groups_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_DELETEGROUPS_RESULT:
        {

                const  rd_kafka_DeleteGroups_result_t *c_delete_groups_res;
                const rd_kafka_group_result_t **c_delete_groups_res_responses;
                size_t c_delete_groups_res_cnt;

                c_delete_groups_res = rd_kafka_event_DeleteGroups_result(rkev);

                c_delete_groups_res_responses =
                        rd_kafka_DeleteConsumerGroupOffsets_result_groups(
                            c_delete_groups_res,
                            &c_delete_groups_res_cnt);

                result = Admin_c_DeleteGroupResults_to_py(c_delete_groups_res_responses,
                                                          c_delete_groups_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT:
        {
                const  rd_kafka_ListConsumerGroupOffsets_result_t *c_list_group_offset_res;
                const rd_kafka_group_result_t **c_list_group_offset_res_responses;
                size_t c_list_group_offset_res_cnt;

                c_list_group_offset_res = rd_kafka_event_ListConsumerGroupOffsets_result(rkev);

                c_list_group_offset_res_responses =
                        rd_kafka_ListConsumerGroupOffsets_result_groups(
                                c_list_group_offset_res,
                                &c_list_group_offset_res_cnt);

                result = Admin_c_GroupResults_to_py(c_list_group_offset_res_responses,
                                                    c_list_group_offset_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT:
        {
                const  rd_kafka_AlterConsumerGroupOffsets_result_t *c_alter_group_offset_res;
                const rd_kafka_group_result_t **c_alter_group_offset_res_responses;
                size_t c_alter_group_offset_res_cnt;

                c_alter_group_offset_res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);

                c_alter_group_offset_res_responses =
                        rd_kafka_AlterConsumerGroupOffsets_result_groups(c_alter_group_offset_res,
                                                                         &c_alter_group_offset_res_cnt);

                result = Admin_c_GroupResults_to_py(c_alter_group_offset_res_responses,
                                                    c_alter_group_offset_res_cnt);

                break;
        }

        default:
                Py_DECREF(error); /* Py_None */
                error = KafkaError_new0(RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                                        "Unsupported event type %s",
                                        rd_kafka_event_name(rkev));
                goto raise;
        }

        if (!result) {
                Py_DECREF(error); /* Py_None */
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
                        PyObject *trace = NULL;

                        /* Fetch (and clear) currently raised exception */
                        PyErr_Fetch(&exctype, &error, &trace);
                        Py_XDECREF(trace);
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
        Py_DECREF(method);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);

        return;

 raise:
        /*
         * Pass an exception to future.set_exception().
         */

        if (!exctype) {
                /* No previous exception raised, use KafkaException */
                exctype = KafkaException;
                Py_INCREF(exctype);
        }

        /* Create a new exception based on exception type and error. */
        excargs = PyTuple_New(1);
        Py_INCREF(error); /* tuple's reference */
        PyTuple_SET_ITEM(excargs, 0, error);
        exc = ((PyTypeObject *)exctype)->tp_new(
                (PyTypeObject *)exctype, NULL, NULL);
        exc->ob_type->tp_init(exc, excargs, NULL);
        Py_DECREF(excargs);
        Py_XDECREF(exctype);
        Py_XDECREF(error); /* from error source above */

        /*
         * Call future.set_exception(exc)
         */
        method = cfl_PyUnistr(_FromString("set_exception"));
        ret = PyObject_CallMethodObjArgs(future, method, exc, NULL);
        Py_XDECREF(ret);
        Py_DECREF(exc);
        Py_DECREF(future);
        Py_DECREF(method);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);
}


static int Admin_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[512];
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

        rd_kafka_conf_set_background_event_cb(conf, Admin_background_event_cb);

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
        "cimpl._AdminClientImpl",   /*tp_name*/
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
        "  Create a new AdminClient instance using the provided configuration dict.\n"
        "\n"
        "This class should not be used directly, use confluent_kafka.AdminClient\n."
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
