import sys
import aerospike
from aerospike import GeoJSON
from aerospike_helpers.operations import map_operations, operations

# Host configuration

config = {
    'hosts': [ ('127.0.0.1', 3100), ('127.0.0.1', 3101), ('127.0.0.1', 3102), ('127.0.0.1', 3103)],
    # Needed for multi-node cluster, returns server configured external IP addresses that client uses to talk to nodes
    "use_services_alternate": True
}

try:
    client = aerospike.client(config).connect()
except Exception as e:
    print(e)
    sys.exit()

write_policy = {'key': aerospike.POLICY_KEY_SEND}
key = ('test', 'ufodata', '5001')

# Create the report map
reportMap = {
    'city': 'Ann Arbor',
    'state': 'Michigan',
    'shape': ['circle', 'flash', 'disc'],
    'duration': '5 minutes',
    'summary': "Large flying disc flashed in the sky above the student union. Craziest thing I've ever seen!"
}

# Format coordinates as a GeoJSON string
geoLoc = GeoJSON({'type':'Point', 'coordinates':[42.2808,83.7430]})

# Create the bins
bins = {
    'occurred': 20220531,
    'reported': 20220601,
    'posted': 20220601,
    # reportMap defined in the section above
    'report': reportMap,
    # geoLoc defined in the section above
    'location': geoLoc
}

# Write the record to Aerospike
client.put(key, bins, policy=write_policy)

# Read key and metadata
read_policy = {'socket_timeout': 300}
(key, meta) = client.exists(key, policy=read_policy)
exists = True
if meta == None:
    exists = False
print('Exists:', exists)
print('Record: ', meta)

# Read whole record
(key, meta, bins) = client.get(key, policy=read_policy)
print('Record: ', bins)

# Read specific bin
(key, meta, bins) = client.select(key, ("report", "location"), policy=read_policy)
print('Record: ', bins)

# Update the record
update_policy = {'exists': aerospike.POLICY_EXISTS_UPDATE}
newPosted = {"posted": 20220602}
client.put(key, newPosted, policy=update_policy)

# Read whole record
(key, meta, bins) = client.get(key, policy=read_policy)
print('Record: ', bins)

# Delete the bin setting it to null
# removeBin = {'posted': aerospike.null()}
# client.put(key, removeBin, policy=update_policy)

# Deleting with durable delete policy prevents deleted objects from resurrection upon a cold restart
# delete_policy = {'durable_delete': True}
# client.remove(key, policy=delete_policy)

# Create map policy
map_policy = {'map_write_flags': aerospike.MAP_WRITE_FLAGS_DEFAULT}

# Create operations
ops = [
    operations.write('posted', 20220602),
    map_operations.map_put('report', 'city', 'Ypsilanti', map_policy),
    operations.read('report')
]
# Update the record
(key_, meta, bins) = client.operate(key, ops)
print('Record: ', bins)

# Close the connection to the server
client.close()