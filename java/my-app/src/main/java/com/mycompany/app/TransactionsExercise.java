package com.mycompany.app;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.ClientPolicy;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;



public final class TransactionsExercise {
    
    public static void main(String[] args) {
        try {
            TransactionsExercise ts = new TransactionsExercise();
            ts.runTest();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AerospikeClient client;
    private EventLoops eventLoops;
    private final Monitor monitor = new Monitor();
    // private final AtomicInteger recordCount = new AtomicInteger();
    private final int numElements = 100;
    private final Key key = new Key("test", "testSet", 123);
    private final int writeTimeout = 5000;
    private final int eventLoopSize = Runtime.getRuntime().availableProcessors();

    public void runTest() throws AerospikeException, Exception {
        // Set logging to Debug
        Log.setCallbackStandard();
        Log.setLevel(Log.Level.DEBUG);
        
        // Create Netty NIO event loop
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = 10;
        eventPolicy.minTimeout = writeTimeout;
        EventLoopGroup group = new NioEventLoopGroup(14);
        eventLoops = new NettyEventLoops(eventPolicy, group, EventLoopType.NETTY_NIO);
        
        // Direct NIO
        // eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);
        
        try {
            // Policies
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.setUseServicesAlternate(true);
            clientPolicy.eventLoops = eventLoops;
            client = new AerospikeClient(clientPolicy, "localhost", 3100);

            try {
                writeRecord();
                monitor.waitTillComplete();
                // Print whole record
                Record record = client.get(null, key);
                System.out.format("Whole record, bins: %s\n", record.bins);
                System.out.println("Completed");
                // System.out.println("Values appended: " + recordCount.get());
                
            } finally {
                client.close();
            }
        } finally {
            eventLoops.close();
        }
    }

    private void writeRecord() throws InterruptedException {
        // Write record
        // Appends 10 elements to list, sleeping for 50ms
        for (int i = 1; i <= numElements; i++){
            EventLoop eventLoop = eventLoops.next();
            // client.operate(writePolicy, key, ListOperation.append("sequence", Value.get(i)));
            client.operate(eventLoop, new MyRecordListener(i), null, key, ListOperation.append("sequence", Value.get(i)));
            Thread.sleep(50);
        }
        
        // client.operate(eventLoop, new MyRecordListener(1), null, key, ListOperation.append("sequence", Value.get(1)));
        // client.operate(null, key, ListOperation.append("sequence", Value.get(1)));
    }

    private class MyRecordListener implements RecordListener {
        private int value;

        public MyRecordListener(int value) {
            this.value = value;
        }

        @Override
        public void onSuccess(Key key, Record record) {
            try {
                System.out.println("Wrote value " + value);
                monitor.notifyComplete();
            }
            catch (Exception e) {
                System.out.println("Error");
                e.printStackTrace();
                monitor.notifyComplete();
            }
        }
        @Override
        public void onFailure(AerospikeException e) {
            e.printStackTrace();
            monitor.notifyComplete();
        }
    }
}
