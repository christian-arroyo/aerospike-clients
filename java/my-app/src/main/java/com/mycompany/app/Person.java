package com.mycompany.app;

import com.aerospike.mapper.annotations.AerospikeBin;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;
import com.aerospike.mapper.annotations.ParamFrom;

@AerospikeRecord(namespace = "test", set = "test_set")
public class Person {
    @AerospikeKey
    @AerospikeBin(name="mybin")
    private String name;
    public Person(@ParamFrom("mybin") String name) {
        super();
        this.name = name;
    }
    public String getName() {
        return name;
    }
}
