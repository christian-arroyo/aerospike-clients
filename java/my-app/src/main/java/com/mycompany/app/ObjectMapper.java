package com.mycompany.app;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.mapper.annotations.AerospikeRecord;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.tools.AeroMapper;

import com.mycompany.app.Person;

public class ObjectMapper {
    public static void main(String[] args) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = 130;
        clientPolicy.minConnsPerNode = 10;
        clientPolicy.readPolicyDefault.totalTimeout = 50;
        clientPolicy.timeout = 50;
        clientPolicy.queryPolicyDefault.totalTimeout = 5;
        clientPolicy.scanPolicyDefault.totalTimeout = 5;
        clientPolicy.setUseServicesAlternate(true);

        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);
        AeroMapper mapper = new AeroMapper.Builder(client).build();
        
        List<Person> records = mapper.scan(Person.class);
        for (Person record : records) {
            System.out.println(record.getName());
        }
        client.close();
    }
}
