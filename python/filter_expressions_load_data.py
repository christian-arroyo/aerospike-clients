"""
Before using filter expressions, make sure to generate the data with asbench

Write 1000 records to the "test" namespace and "benchdata" set containing a random 16 character string
bin, followed by a list bin with a random double, integer, and 4 character string.

Command:
    asbench -h 127.0.0.1 -p 3100 -n test -s benchdata -k 1000 -o "S16,[D,I,S4]" -w I -R

The data has the following structure:

aql> select * from test.benchdata where pk = 1000
+------+--------------------+-------------------------------------------------------+
| PK   | testbin            | testbin_2                                             |
+------+--------------------+-------------------------------------------------------+
| 1000 | "jkya3y1x9z0gftr2" | LIST('[-1.617098065020263e+259, 1569199873, "6oi7"]') |
+------+--------------------+-------------------------------------------------------+
"""

import aerospike
from aerospike_helpers import expressions as exp


# Define host configuration
config = {
    'hosts': [ ('127.0.0.1', 3100)],
    # Needed for multi-node cluster, returns server configured external IP addresses that client uses to talk to nodes
    "use_services_alternate": True
}
# Make client connection
client = aerospike.client(config).connect()

# Write player records to database
keys = [("test", "demo", i) for i in range(1, 5)]
records = [
            {'user': "Chief"  , 'scores': [6, 12, 4, 21], 'kd': 1.2},
            {'user': "Arbiter", 'scores': [5, 10, 5, 8] , 'kd': 1.0},
            {'user': "Johnson", 'scores': [8, 17, 20, 5], 'kd': 0.9},
            {'user': "Regret" , 'scores': [4, 2, 3, 5]  , 'kd': 0.3}
        ]
for key, record in zip(keys, records):
    client.put(key, record)

# Create the query
query = client.query('test', 'demo')

# Set max records to return
query.max_records = 20

# Create callback function
def record_set(record):
    (key, meta, bins) = record
    # Do something
    print('Key: {0} | Record: {1}'.format(key[2], bins))

# Execute the query
query.foreach(record_set)
client.close()