import aerospike

import sys

# from __future__ import print_function 
# aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG) 
# aerospike.set_log_handler(callback)

config = {
    'hosts': [ ('localhost', 3100), ('localhost', 3101), ('localhost', 3102) ],
    'use_services_alternate': True,
    'rack_aware': True,
    'rack_id': 1
}

aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)

try:
    client = aerospike.client(config).connect()
except Exception as e:
    print("Error: {0} {1}".format(e.msg, e.code))
    sys.exit(1)

keyTuple = ('test', 'ra_set', 200)

# Write record
client.put(keyTuple, {'name': 'John Doe', 'age': 32})
read_policy = {'read': aerospike.POLICY_READ_MODE_AP_ALL, 'replica': aerospike.POLICY_REPLICA_PREFER_RACK}

# Read record
(key, meta, record) = client.get(keyTuple, read_policy)
print(key)
print(meta)
print(record)

client.close()