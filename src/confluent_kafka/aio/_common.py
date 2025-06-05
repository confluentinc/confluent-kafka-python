# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio


class AsyncLogger:

    def __init__(self, loop, logger):
        self.loop = loop
        self.logger = logger

    def log(self, *args, **kwargs):
        self.loop.call_soon_threadsafe(self.logger.log, *args, **kwargs)


def wrap_callback(loop, callback, edit_args=None, edit_kwargs=None):
    def ret(*args, **kwargs):
        if edit_args:
            args = edit_args(args)
        if edit_kwargs:
            kwargs = edit_kwargs(kwargs)
        f = asyncio.run_coroutine_threadsafe(callback(*args, **kwargs),
                                             loop)
        return f.result()
    return ret


def wrap_conf_callback(loop, conf, name):
    if name in conf:
        cb = conf[name]
        conf[name] = wrap_callback(loop, cb)


def wrap_conf_logger(loop, conf):
    if 'logger' in conf:
        conf['logger'] = AsyncLogger(loop, conf['logger'])


def wrap_common_callbacks(loop, conf):
    wrap_conf_callback(loop, conf, 'error_cb')
    wrap_conf_callback(loop, conf, 'throttle_cb')
    wrap_conf_callback(loop, conf, 'stats_cb')
    wrap_conf_callback(loop, conf, 'oauth_cb')
    wrap_conf_logger(loop, conf)
