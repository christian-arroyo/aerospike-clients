package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.Log;

import java.lang.Thread;


public class Ops7625 {
    public static void main(String[] args) {
        // Client Connection
        Log.setCallbackStandard();
        Log.setLevel(Log.Level.DEBUG);

        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        clientPolicy.timeout = 20;
        
        clientPolicy.maxConnsPerNode = 8000; // Default is 100
        // clientPolicy.ConnectionQueueSize = 8000;

        //clientPolicy.minConnsPerNode = 5000;
        clientPolicy.minConnsPerNode = 5000; // Default is 0

        // Also tried with the below set to 3600 * time.second
        clientPolicy.setMaxSocketIdle(10); // Default is 0
        // clientPolicy.IdleTimeout = 0;

        System.out.println("Connect");
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);

        System.out.println("Waiting");
        while (true) {
            // stats, err := client.stats();
            ClusterStats stats = client.getClusterStats(); 
            System.out.println(stats);
            try {
                // Sleep for 2 minutes
                Thread.sleep(1000 * 5);
              } catch (InterruptedException e) {
                System.out.format("Interrupted: %s", e);
                Thread.currentThread().interrupt();
              } 
        }
    }
}
