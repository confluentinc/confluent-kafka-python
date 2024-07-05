import sys
import logging

from confluent_kafka.admin import AdminClient

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s <broker>\n" % sys.argv[0])
    sys.exit(1)

broker = sys.argv[1]

# Custom logger
logger = logging.getLogger('AdminClient')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Admin client with logger
a = AdminClient({'bootstrap.servers': broker,
                 'debug': 'all'},
                logger=logger)

# Alternatively, pass the logger as a key.
# When passing it as an argument, it overwrites the key.
#
# a = AdminClient({'bootstrap.servers': broker,
#                  'debug': 'all',
#                  'logger': logger})

# Sample Admin API call
future = a.list_consumer_groups(request_timeout=10)

while not future.done():
    # Log messages through custom logger while waiting for the result
    a.poll(0.1)

try:
    list_consumer_groups_result = future.result()
    print("\n\n\n========================= List consumer groups result Start =========================")
    print("{} consumer groups".format(len(list_consumer_groups_result.valid)))
    for valid in list_consumer_groups_result.valid:
        print("    id: {} is_simple: {} state: {}".format(
            valid.group_id, valid.is_simple_consumer_group, valid.state))
    print("{} errors".format(len(list_consumer_groups_result.errors)))
    for error in list_consumer_groups_result.errors:
        print("    error: {}".format(error))
    print("========================= List consumer groups result End =========================\n\n\n")

except Exception:
    raise

# Log final log messages
a.poll(0)
