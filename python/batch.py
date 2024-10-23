import aerospike
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import expression_operations, map_operations, operations
from aerospike_helpers.batch import records as br

config = {
    "hosts": [("localhost", 3100), ("localhost", 3101), ("localhost", 3102)],
    "use_services_alternate": True
}
client = aerospike.client(config).connect()

# An example that will always return true
expr = exp.GT(2, 1).compile()

# Create a new batch policy
batch_policy = {'expressions': expr}

# Create the batch write policy
batch_write_policy = {'expressions': expr}

# Exists - Creates an array of ten keys and checks for their existance in the database

keys = []
for i in range(4995, 5006):
    batch_key = ('test', 'a_set', i)
    keys.append(batch_key)

# Check if record exists
exists = client.exists_many(keys)

# Access the records
for record in exists:
    (key, meta) = record
    if meta == None:
        print('Key: ', key[2], ' does not exist\\n')

# Create batch of keys
keys = []
for i in range(1,11):
    batch_key = ('test', 'ufodata', i)
    keys.append(batch_key)

# Read each whole record 
records = client.get_many(keys);

# Or specifiy bins 
# records = client.select_many(keys, ("report", "location"));

# Access the records 
for record in records:
    (key, meta, bins) = record
    # Do something
    print('Record: ', bins, '\\n')

client.close()
