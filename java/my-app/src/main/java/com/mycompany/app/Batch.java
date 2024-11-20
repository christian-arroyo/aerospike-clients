package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.ClientPolicy;

import java.util.Arrays;
import java.util.List;

public class Batch {
    public static void main(String[] args) {
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);
        
        // Batch Policy
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.filterExp = Exp.build(
            // Always returns true, 2 > 1
            Exp.gt(Exp.val(2), Exp.val(1))
        );

        // Batch Write Policy
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.filterExp = Exp.build(
            Exp.gt(Exp.val(2), Exp.val(1))
        );

        // Create an array of ten keys and check for their existence in the database
        Key[] keys = new Key[10];
        for (int i = 0; i < 10; i++) {
            keys[i] = new Key("test", "test", i + 4995);
        }
        boolean[] exists = client.exists(batchPolicy, keys);

        for (int i = 0; i < exists.length; i++) {
            System.out.format("Key: %s does not exist\n", keys[i].userKey);
        }
        client.close();
    }
}
