package com.mycompany.app;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;

public final class AsyncTest {

    public static void main(String[] args) {
        try {
            AsyncTest test = new AsyncTest();
            test.runTest();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AerospikeClient client;
    private EventLoops eventLoops;
    private final Monitor monitor = new Monitor();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final int maxCommandsInProcess = 40;
    // private final int recordMax = 100000;
    private final int recordMax = 100000;
    private final int writeTimeout = 5000;
    private final int eventLoopSize;
    private final int concurrentMax;

    public AsyncTest() {
        // Allocate an event loop for each cpu core.
        eventLoopSize = Runtime.getRuntime().availableProcessors();
        System.out.println("eventLoopSize: " + eventLoopSize);

        // Set total concurrent commands for all event loops.
        concurrentMax = eventLoopSize * maxCommandsInProcess;
        System.out.println("concurrentMax: " + concurrentMax);
    }

    public void runTest() throws AerospikeException, Exception {
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.minTimeout = writeTimeout;

        // This application uses it's own external throttle (Start with concurrentMax
        // commands and only start one new command after previous command completes),
        // so setting EventPolicy maxCommandsInProcess is not necessary.
        // eventPolicy.maxCommandsInProcess = maxCommandsInProcess;

        // Direct NIO
        eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);

        // Netty NIO
        // EventLoopGroup group = new NioEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        // Netty epoll (Linux only)
        // EventLoopGroup group = new EpollEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        try {
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.setUseServicesAlternate(true);
            clientPolicy.eventLoops = eventLoops;

            // maxConnsPerNode needs to be increased from default (300)
            // if eventLoopSize >= 8.
            clientPolicy.maxConnsPerNode = concurrentMax;
            clientPolicy.writePolicyDefault.setTimeout(writeTimeout);
            client = new AerospikeClient(clientPolicy, "localhost", 3100);

            try {
                writeRecords();
                monitor.waitTillComplete();
                System.out.println("Records written: " + recordCount.get());
            }
            finally {
                client.close();
            }
        }
        finally {
            eventLoops.close();
        }
    }

    private void writeRecords() {
        // Write exactly concurrentMax commands to seed event loops.
        // Distribute seed commands across event loops.
        // A new command will be initiated after each command completion in WriteListener.
        for (int i = 1; i <= concurrentMax; i++) {
            EventLoop eventLoop = eventLoops.next();
            writeRecord(eventLoop, new AWriteListener(eventLoop), i);
        }
    }

    private void writeRecord(EventLoop eventLoop, WriteListener listener, int keyIndex) {
        Key key = new Key("test", "test", keyIndex);
        Bin bin = new Bin("bin", keyIndex);
        client.put(eventLoop, listener, null, key, bin);
    }

    private class AWriteListener implements WriteListener {
        private final EventLoop eventLoop;

        public AWriteListener(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        @Override
        public void onSuccess(Key key) {
            try {
                int count = recordCount.incrementAndGet();

                // Stop if all records have been written.
                if (count >= recordMax) {
                    monitor.notifyComplete();
                    return;
                }

                if (count % 10000 == 0) {
                    System.out.println("Records written: " + count);
                }

                // Issue one new command if necessary.
                int keyIndex = concurrentMax + count;
                if (keyIndex <= recordMax) {
                    // Write next record on same event loop.
                    writeRecord(eventLoop, this, keyIndex);
                }
            }
            catch (Exception e) {
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
