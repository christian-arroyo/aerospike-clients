package com.mycompany.app;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.lang.Thread;


public class Ops7625 {
    public static void main(String[] args) {
        Log.setCallbackStandard();
        Log.setLevel(Log.Level.DEBUG);
        // ClientPolicy clientPolicy = new ClientPolicy();
        // clientPolicy.useServicesAlternate = true;
        // clientPolicy.timeout = 20;
        // clientPolicy.maxConnsPerNode = 8000; // Default is 100
        // clientPolicy.minConnsPerNode = 100; // Default is 0
        // clientPolicy.setMaxSocketIdle(0); // Default is 0

        System.out.println("Connect");
        // AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);

        EventPolicy eventPolicy = new EventPolicy();
        EventLoopGroup group = new NioEventLoopGroup(1);
        EventLoops eventLoops = new NettyEventLoops(eventPolicy, group, EventLoopType.NETTY_NIO);

        ClientPolicy policy = new ClientPolicy();
        policy.useServicesAlternate = true;
        policy.eventLoops = eventLoops;
        policy.asyncMinConnsPerNode = 100;
        policy.asyncMaxConnsPerNode = 500;
        IAerospikeClient client = new AerospikeClient(policy, "localhost", 3100);

        System.out.println("Waiting");
        while (true) {
            ClusterStats stats = client.getClusterStats(); 
            System.out.println(stats);
            try {
                Thread.sleep(1000 * 5);
              } catch (InterruptedException e) {
                System.out.format("Interrupted: %s", e);
                Thread.currentThread().interrupt();
              } 
        }
    }
}
