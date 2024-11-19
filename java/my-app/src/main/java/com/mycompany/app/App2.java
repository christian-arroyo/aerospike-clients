package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.RecordSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.events.Namespace;

import java.util.List;

public class App2 {
    public static void main(String[] args) {
        
        // Connection
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        AerospikeClient client = new AerospikeClient(clientPolicy, "127.0.0.1", 3100);
        
        Policy policy = new Policy();
        policy.socketTimeout = 0;

        Statement statement = new Statement();
        statement.setNamespace("test");
        statement.setSetName("asbench");
        statement.setMaxRecords(2);
        statement.setBinNames("testbin");
        System.out.format("Query on ns=%s set=%s, with bin %s", "test", "asbench", "testbin\n");
        RecordSet rs = client.query(null, statement);

        while (rs.next()) {
            Key key = rs.getKey();
            Record record = rs.getRecord();
            System.out.format("key=%s bins=%s\n", key.userKey, record.bins);
        }
        rs.close();
        client.close();
    }
}
