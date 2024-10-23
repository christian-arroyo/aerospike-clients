import aerospike
from aerospike_helpers.operations import map_operations
from aerospike_helpers import cdt_ctx
from aerospike import predicates

import pprint
import json

# Setup
config = {
    "hosts": [("localhost", 3100), ("localhost", 3101), ("localhost", 3102)],
    "use_services_alternate": True
}
client = aerospike.client(config).connect()

# Aerospike namespace, set, and key_id to be used for the Aerospike key
namespace = "test"
as_set = "table1"
key_id = 6

# define Aerospike key
key = (namespace, as_set, key_id)

# example JSON string
employees = """[{"id": "09", "name": "Nitin", "department": "Finance"},
                {"id": "10", "name": "Jonathan", "department": "Human Resources"},
                {"id": "11", "name": "Caitlin", "department": "Engineering"}]"""

# Convert string to Python dict
employee_dict = json.loads(employees)
pprint.pprint(employee_dict)

# Set an Aerospike bin with the bin name "employees" and put
# the JSON document in as a list.
bins = {"employees": employee_dict}

# Create the write policy
write_policy = {"key": aerospike.POLICY_KEY_SEND}

# Write the record to Aerospike
try:
    client.put(key=key, bins=bins, policy=write_policy)

    (key_, meta, bins) = client.get(key=key)
    print("Create succeeded\\nKey: ", key[2], "\\nRecord:")
    pprint.pprint(bins)
except aerospike.exception.AerospikeError as e:
    print(f"Create failed\\nError:{e.msg},{e.code}")

# Create the update request. Update the employee at index[2]'s
# department from "Engineering" to "Support".
ctx = [cdt_ctx.cdt_ctx_list_index(2)]
ops = [
    map_operations.map_put(bin_name="employees", key="department", value="Support", ctx=ctx)
]
try:
    ret_key, ret_meta, ret_bins = client.operate(key=key, list=ops)
except aerospike.exception.AerospikeError as e:
    print(f"Update failed\\nError:{e.msg},{e.code}")
else:
    print("Updated record successfully:")
    pprint.pprint(ret_bins)

# read the 'department' value for from the employees map list's last map
ctx = [cdt_ctx.cdt_ctx_list_index(2)]
ops = [
    map_operations.map_get_by_key(bin_name="employees", key="department", return_type=aerospike.MAP_RETURN_VALUE,
                                  ctx=ctx)
]
ret_key, ret_meta, ret_bins = client.operate(key=key, list=ops)
print("element 2's value in their 'department' field: ")
pprint.pprint(ret_bins)


# Secondary Index Queries

transactions = """[{"txn_id":"1111", "name": "Davis", "item_id":"A1234", "count":1},
                  {"txn_id":"2222", "name": "Johnson", "item_id":"B2345", "count":2},
                  {"txn_id":"3333", "name": "Johnson", "item_id":"C3456", "count":2},
                  {"txn_id":"4444", "name": "Lee", "item_id":"D4567", "count":3}]"""

transactions_dict = json.loads(transactions)
pprint.pprint(transactions_dict)

# Method 1 - Extracts secondary index target during insertion of the document, then uses it to query
for transaction in transactions_dict:
    bins = {"name": transaction["name"], "transaction": transaction}
    key = (namespace, as_set, transaction["txn_id"])
    # Write the record to Aerospike
    try:
        # Insert the record
        client.put(key, bins, policy=write_policy)
        (key_, meta, bins) = client.get(key)
        print("Create succeeded\\nKey: ", key[2], "\\nRecord:")
        pprint.pprint(bins)
    except aerospike.exception.AerospikeError as e:
        print("Create failed\\nError: {0} [{1}]".format(e.msg, e.code))

# Method 2 - Uses the document bin directly for the query
try:
    client.index_map_values_create(namespace, as_set, "transaction", aerospike.INDEX_STRING, "test_transaction_idx")
except aerospike.exception.AerospikeError as e:
    print("Create index failed\\nError: {0} [{1}]".format(e.msg, e.code))
else:
    print("Successfully created index test_transaction_idx on the 'transaction' bin")

# Set up a query to use the secondary index
test_transaction_idx_qry = client.query(namespace, as_set)

# Add the projection
test_transaction_idx_qry.select("name", "transaction")

# Add the query predicate
test_transaction_idx_qry.where(predicates.contains("transaction", aerospike.INDEX_TYPE_MAPVALUES, "Johnson"))

# Define callback function
def print_record(result_tuple):
    print(result_tuple)
    return
    
# Execute query
test_transaction_idx_qry.foreach(print_record)

# Clean up index
client.index_remove(namespace, "test_transaction_idx")
