import pytest
import confluent_kafka
import os
import uuid
import sys
import json

# Default test conf location
testconf_file = "testconf.json"

# Kafka bootstrap server(s)
bootstrap_servers = None

# Topic prefix to use
topic = None

# API version requests are only implemented in Kafka broker >=0.10
# but the client handles failed API version requests gracefully for older
# versions as well, except for 0.9.0.x which will stall for about 10s
# on each connect with this set to True.
api_version_request = True

# global variable to be set by stats_cb call back function
good_stats_cb_result = False

# global counter to be incremented by throttle_cb call back function
throttled_requests = 0

# global variable to track garbage collection of suppressed on_delivery callbacks
DrOnlyTestSuccess_gced = 0

# Shared between producer and consumer tests and used to verify
# that consumed headers are what was actually produced.


def abort_on_missing_configuration(name):
    raise ValueError("{} configuration not provided. Aborting test...".format(name))


def verify_avro_https(mode_conf):
    if mode_conf is None:
        abort_on_missing_configuration('avro-https')

    base_conf = dict(mode_conf, **{'bootstrap.servers': bootstrap_servers,
                                   'error_cb': error_cb})

    consumer_conf = dict(base_conf, **{'group.id': generate_group_id(),
                                       'session.timeout.ms': 6000,
                                       'enable.auto.commit': False,
                                       'on_commit': print_commit_result,
                                       'auto.offset.reset': 'earliest'})

    run_avro_loop(base_conf, consumer_conf)


def run_avro_loop(producer_conf, consumer_conf):
    from confluent_kafka import avro
    avsc_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'avro')

    p = avro.AvroProducer(producer_conf)

    prim_float = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
    prim_string = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
    basic = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
    str_value = 'abc'
    float_value = 32.0

    combinations = [
        dict(key=float_value, key_schema=prim_float),
        dict(value=float_value, value_schema=prim_float),
        dict(key={'name': 'abc'}, key_schema=basic),
        dict(value={'name': 'abc'}, value_schema=basic),
        dict(value={'name': 'abc'}, value_schema=basic, key=float_value, key_schema=prim_float),
        dict(value={'name': 'abc'}, value_schema=basic, key=str_value, key_schema=prim_string),
        dict(value=float_value, value_schema=prim_float, key={'name': 'abc'}, key_schema=basic),
        dict(value=float_value, value_schema=prim_float, key=str_value, key_schema=prim_string),
        dict(value=str_value, value_schema=prim_string, key={'name': 'abc'}, key_schema=basic),
        dict(value=str_value, value_schema=prim_string, key=float_value, key_schema=prim_float),
        # Verify identity check allows Falsy object values(e.g., 0, empty string) to be handled properly (issue #342)
        dict(value='', value_schema=prim_string, key=0.0, key_schema=prim_float),
        dict(value=0.0, value_schema=prim_float, key='', key_schema=prim_string),
    ]

    for i, combo in enumerate(combinations):
        combo['topic'] = str(uuid.uuid4())
        combo['headers'] = [('index', str(i))]
        p.produce(**combo)
    p.flush()

    c = avro.AvroConsumer(consumer_conf)
    c.subscribe([(t['topic']) for t in combinations])

    msgcount = 0
    while msgcount < len(combinations):
        msg = c.poll(1)

        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        tstype, timestamp = msg.timestamp()
        print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s' %
              (msg.topic(), msg.partition(), msg.offset(),
               msg.key(), msg.value(), tstype, timestamp))

        # omit empty Avro fields from payload for comparison
        record_key = msg.key()
        record_value = msg.value()
        index = int(dict(msg.headers())['index'])

        if isinstance(msg.key(), dict):
            record_key = {k: v for k, v in msg.key().items() if v is not None}

        if isinstance(msg.value(), dict):
            record_value = {k: v for k, v in msg.value().items() if v is not None}

        assert combinations[index].get('key') == record_key
        assert combinations[index].get('value') == record_value

        c.commit()
        msgcount += 1

    # Close consumer
    c.close()


def print_usage(exitcode, reason=None):
    """ Print usage and exit with exitcode """
    if reason is not None:
        print('Error: %s' % reason)
    sys.exit(exitcode)


def resolve_envs(_conf):
    """Resolve environment variables"""

    for k, v in _conf.items():
        if isinstance(v, dict):
            resolve_envs(v)

        if str(v).startswith('$'):
            _conf[k] = os.getenv(v[1:])


def error_cb(err):
    print('Error: %s' % err)


def generate_group_id():
    return str(uuid.uuid1())


def print_commit_result(err, partitions):
    if err is not None:
        print('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        print('# Committed offsets for: %s' % partitions)


def test_avro_https():
    with open(testconf_file) as f:
        testconf = json.load(f)
        resolve_envs(testconf)

    bootstrap_servers = testconf.get('bootstrap.servers')
    topic = testconf.get('topic')

    if bootstrap_servers is None or topic is None:
        print_usage(1, "Missing required property bootstrap.servers")

    if topic is None:
        print_usage(1, "Missing required property topic")

    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())
    print('Brokers: %s' % bootstrap_servers)
    print('Topic prefix: %s' % topic)

    print('=' * 30, 'Verifying AVRO with HTTPS', '=' * 30)
    verify_avro_https(testconf.get('avro-https', None))
    key_with_password_conf = testconf.get("avro-https-key-with-password", None)
    print(key_with_password_conf)
    print('=' * 30, 'Verifying AVRO with HTTPS Flow with Password',
          'Protected Private Key of Cached-Schema-Registry-Client', '=' * 30)
    verify_avro_https(key_with_password_conf)
    print('Verifying Error with Wrong Password of Password Protected Private Key of Cached-Schema-Registry-Client')
    try:
        key_with_password_conf['schema.registry.ssl.key.password'] += '->wrongpassword'
        verify_avro_https(key_with_password_conf)
        print_usage(1, "Wrong Password Does not affect the Connection of Password Protected SSL Key")
    except Exception:
        print("Wrong Password Gives Error -> Successful")
    print("Reached here")
    sys.exit(0)


if __name__ == '__main__':
    """Run test suites"""
    print("Start the test!!")