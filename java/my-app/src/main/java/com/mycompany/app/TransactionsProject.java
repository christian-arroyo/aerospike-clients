package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Record;

public class TransactionsProject {
    public static void main(String[] args) throws InterruptedException {    
        
        int numElements = 100;

        // Policies
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.setUseServicesAlternate(true);
        WritePolicy writePolicy = new WritePolicy();

        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);
        Key key = new Key("test", "testSet", 123);
        // Write record
        // Appends 10 elements to list, sleeping for 50ms
        for (int i = 1; i <= numElements; i++){
            client.operate(writePolicy, key, ListOperation.append("sequence", Value.get(i)));
            Thread.sleep(50);
        }
        // Read policy
        Policy policy = new Policy();
        policy.socketTimeout = 300;
        Record record = client.get(null, key);
        System.out.format("Whole record, bins: %s\n", record.bins);
        client.close();
    }
}
