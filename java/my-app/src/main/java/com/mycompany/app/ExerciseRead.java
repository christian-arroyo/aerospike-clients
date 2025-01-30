package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;

public class ExerciseRead {
    public static void main(String[] args){
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        
        // Read policy
        Policy policy = new Policy();
        policy.socketTimeout = 300;
        // Establish a connection to the server
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);

        try{
            Key key = new Key("test", "testSet", 123);
            // Get whole record
            Record record = client.get(policy, key);
            System.out.format("Whole record, bins: %s\n", record.bins);
        } finally {
            client.close();
        }
    }
}
