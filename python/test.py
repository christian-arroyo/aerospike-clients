import aerospike
from aerospike_helpers.operations import map_operations

import pprint
import json

# set aerospike host config
config = {
    "hosts": [("localhost", 3100), ("localhost", 3101), ("localhost", 3102)],
    "use_services_alternate": True
}

# create the aerospike client and connect
client = aerospike.client(config).connect()

# aerospike namespace, set, and key_id to be used for the aerospike key
namespace = "test"
set = "table1"
key_id = 5

# define aerospike key
key = (namespace, set, key_id)

# example JSON string
employee ='{"id":"09", "name": "Nitin", "department":"Finance"}'

# Convert string to Python dict
employee_dict = json.loads(employee)
pprint.pprint(employee_dict)

# set an aerospike bin with the bin name "employee" and
# put the JSON document in as a map
bins = {
    "employee": employee_dict
}

# Create the write policy
write_policy = {"key": aerospike.POLICY_KEY_SEND}

# Write the record to Aerospike
try:
    client.put(key=key, bins=bins, policy=write_policy)

    (key_, meta, bins) = client.get(key=key)
    print("Create succeeded\nKey: ", key[2], "\nRecord:")
    pprint.pprint(bins)

except aerospike.exception.AerospikeError as e:
    print(f"Create failed\nError: {e.msg} [{e.code}]")