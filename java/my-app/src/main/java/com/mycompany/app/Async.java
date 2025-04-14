package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import java.util.concurrent.atomic.AtomicInteger;

public class Async {

    private static final AtomicInteger callbackCount = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Log.setLevel(Log.Level.DEBUG);
        // Aerospike configuration
        String host = "localhost";
        int port = 3102;
        String namespace = "test";
        String set = "myset";

        // Event loop setup
        EventPolicy eventPolicy = new EventPolicy();
        EventLoops eventLoops = new NioEventLoops(eventPolicy, 5);

        // Client setup
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        clientPolicy.setUseServicesAlternate(true);

        AerospikeClient client = null;
        try {
            client = new AerospikeClient(clientPolicy, host, port);

            // Scan policy
            ScanPolicy scanPolicy = new ScanPolicy();
            scanPolicy.concurrentNodes = true;
            // scanPolicy.concurrentNodes = false;

            // Perform async scan
            client.scanAll(eventLoops.next(), new RecordSequenceListener() {
                private final int callbackId = callbackCount.incrementAndGet();

                @Override
                public void onRecord(Key key, Record record) {
                    System.out.println("Callback ID: " + callbackId +
                            " | Key: " + key.userKey);
                }

                @Override
                public void onSuccess() {
                    System.out.println("Scan completed for callback ID: " + callbackId);
                    System.out.println("Async Callback on thread id: " + Thread.currentThread().getId());
                }

                @Override
                public void onFailure(AerospikeException e) {
                    System.err.println("Scan failed for callback ID: " + callbackId);
                    e.printStackTrace();
                }
            }, scanPolicy, namespace, set);

            // Wait for scan to complete
            Thread.sleep(3000);

            System.out.println("\nTotal async callbacks executed: " + callbackCount.get());

            // Perform sync scan. Prints the callback thread id
            client.scanAll(scanPolicy,"test","myset",((key, record) -> {
                System.out.println("Sync Callback on thread id:"+Thread.currentThread().getId());
            }));

        } finally {
            if (client != null) {
                client.close();
            }
            eventLoops.close();
        }
    }
}
