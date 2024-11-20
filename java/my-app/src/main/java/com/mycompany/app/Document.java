package com.mycompany.app;

import java.util.Map;
import java.util.HashMap;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.google.gson.Gson;


public class Document {
    public static void main(String[] args) {

        // Create client
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);
        
        String namespace = "test";
        String set = "table2";
        int key_id = 100000000;
        Key key = new Key(namespace, set, key_id);

        // example JSON string
        String employee = "{'id':'09', 'name': 'Nitin', 'department':'Finance'}";

        // Convert string to Java Map
        Map<String, Object> employeeMap = new HashMap<String, Object>();
        employeeMap = new Gson().fromJson(employee, employeeMap.getClass());

        // Write document to Aerospike
        Bin bin = new Bin("employee", Value.get(employeeMap));
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        // Write the record to Aerospike
        try {
            client.put(writePolicy, key, bin);
            Record record = client.get(null, key);
            System.out.println("Create succeeded\nKey: " + key + "\nRecord: " + 
            record.getValue(employee));        }
        catch (AerospikeException ae) {
            System.out.println("Create failed\nError: " + ae.getMessage());
        }
        finally {
            client.close();
        }
    }
}
