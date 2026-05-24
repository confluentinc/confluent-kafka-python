#!/usr/bin/env python
"""Smoke check that all five ShareConsumer callbacks wire up correctly:
error_cb, log_cb, throttle_cb, stats_cb (rejected), oauth_cb.

Hermetic — uses localhost:19999 (unreachable). Prints visible
KR-prefixed lines plus a summary so you can eyeball each channel.

Callback shapes covered:

- error_cb / log_cb — fire many times during the poll loop against the
  unreachable broker. Counts shown in summary.
- throttle_cb — registered, but requires broker-side throttling to fire.
  Count stays 0 against an unreachable broker (expected). See
  tests/integration/share_consumer/test_share_consumer_callbacks.py for
  the "actually fires" test.
- stats_cb — REJECTED at construction time on ShareConsumer (Layer 2).
  We verify the rejection by trying to set it and catching the ValueError.
- oauth_cb — fires on librdkafka's background thread during
  ShareConsumer init (rd_kafka_share_sasl_background_callbacks_enable +
  wait_for_oauth_token_set). Requires librdkafka built with OpenSSL;
  on builds without it, OAUTHBEARER config is rejected at conf-set time
  and we skip with a clear note.

Not a pytest test; just `python cb_smoke.py` and read the output.
"""
import logging
import time

from confluent_kafka import KafkaException, Producer, ShareConsumer


# ---------------------------------------------------------------------------
# Stats rejection check (Layer 2 — confluent_kafka.c inline-strcmp branch)
# ---------------------------------------------------------------------------
print('-- stats_cb rejection check --')
try:
    ShareConsumer({
        'group.id': 'cb-smoke-stats',
        'bootstrap.servers': 'localhost:19999',
        'stats_cb': lambda j: None,
    })
    print('KR: STATS-FAIL | construction unexpectedly succeeded')
except ValueError as e:
    print(f'KR: STATS-REJECTED | {e}')

try:
    ShareConsumer({
        'group.id': 'cb-smoke-stats-interval',
        'bootstrap.servers': 'localhost:19999',
        'statistics.interval.ms': 200,
    })
    print('KR: STATS-INTERVAL-FAIL | construction unexpectedly succeeded')
except ValueError as e:
    print(f'KR: STATS-INTERVAL-REJECTED | {e}')


# ---------------------------------------------------------------------------
# OAuth check (Task #22 — share_sasl_background_callbacks_enable +
#                          wait_for_oauth_token_set in ShareConsumer_init)
# ---------------------------------------------------------------------------
def _librdkafka_has_openssl():
    try:
        Producer({'security.protocol': 'sasl_ssl'})
    except KafkaException as exc:
        if 'OpenSSL not available' in str(exc):
            return False
        raise
    return True


print()
print('-- oauth_cb check --')
oauth_count = [0]


def my_oauth_cb(oauth_config):
    oauth_count[0] += 1
    print(
        f'KR: OAUTH | invocation={oauth_count[0]} '
        f'oauth_config={oauth_config!r}'
    )
    return 'token', time.time() + 300.0


if not _librdkafka_has_openssl():
    print(
        'KR: OAUTH-SKIP | librdkafka built without OpenSSL; '
        'OAUTHBEARER config gated at conf-set time. Wiring tested by '
        'test_oauthbearer_token_refresh_cb in CI.'
    )
else:
    sc_oauth = ShareConsumer({
        'group.id': 'cb-smoke-oauth',
        'bootstrap.servers': 'localhost:19999',
        'socket.timeout.ms': 100,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.config': 'oauth_cb',
        'oauth_cb': my_oauth_cb,
    })
    # By the time the constructor returns, the background thread has
    # already invoked oauth_cb and set the token — that's what
    # wait_for_oauth_token_set blocks on.
    if oauth_count[0] >= 1:
        print(
            f'KR: OAUTH-OK | oauth_cb fired {oauth_count[0]} '
            f'time(s) during ShareConsumer init'
        )
    else:
        print('KR: OAUTH-FAIL | constructor returned without oauth_cb firing')
    sc_oauth.close()


# ---------------------------------------------------------------------------
# Main poll loop — error_cb, log_cb, throttle_cb
# ---------------------------------------------------------------------------
print()
print('-- main poll loop (error_cb / log_cb / throttle_cb) --')

# Mirror examples/consumer.py:80-85 setup
logger = logging.getLogger('share-cb-smoke')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('KR: LOG | %(message)s'))
logger.addHandler(handler)

err_count = [0]
throttle_count = [0]


def my_error_cb(err):
    err_count[0] += 1
    print(f'KR: ERR | code={err.code()} name={err.name()} str={err}')


def my_throttle_cb(event):
    throttle_count[0] += 1
    print(
        f'KR: THROTTLE | broker_name={event.broker_name} '
        f'broker_id={event.broker_id} throttle_time={event.throttle_time}'
    )


sc = ShareConsumer({
    'group.id': 'cb-smoke-group',
    'bootstrap.servers': 'localhost:19999',  # unreachable on purpose
    'socket.timeout.ms': 100,
    'debug': 'broker,fetch',                 # turn on librdkafka debug
    'error_cb': my_error_cb,
    'throttle_cb': my_throttle_cb,
    'logger': logger,
})

sc.subscribe(['cb-smoke-topic'])

for i in range(5):
    sc.poll(timeout=0.5)
    print(f'-- poll #{i+1} done --')

sc.close()
print('-- closed --')


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
print('-- summary --')
print(f'   stats_cb               : rejected at construction (Layer 2)')
print(f'   statistics.interval.ms : rejected at construction (Layer 2)')
if _librdkafka_has_openssl():
    print(f'   oauth_cb               : fired {oauth_count[0]} times during init')
else:
    print(f'   oauth_cb               : skipped (no OpenSSL)')
print(f'   error_cb               : fired {err_count[0]} times across 5 polls')
print(f'   throttle_cb            : fired {throttle_count[0]} times (0 expected against unreachable broker)')
print(f'   log_cb                 : fired (visible as KR: LOG | ... lines above)')
