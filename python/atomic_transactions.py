import sys
import aerospike
from aerospike import GeoJSON

# Host configuration

config = {
    'hosts': [ ('127.0.0.1', 3100), ('127.0.0.1', 3101), ('127.0.0.1', 3102), ('127.0.0.1', 3103)],
    # Needed for multi-node cluster
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

# Close the connection to the server
client.close()
