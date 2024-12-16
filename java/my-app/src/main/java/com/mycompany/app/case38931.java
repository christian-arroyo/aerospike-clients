package com.mycompany.app;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Record;


public class case38931 {
    public static void main(String[] args) {
        // Connection
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);

        // Write Policy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.setSendKey(true); // send user-defined key in addition to hash digest on both r/w

        Key key = new Key("test", "test", 12345);
        Record result = client.operate(
            writePolicy, 
            key,
            MapOperation.create("test_bin", MapOrder.KEY_ORDERED),
            MapOperation.put(
                MapPolicy.Default,
                "test_bin",
                Value.get(1),
                Value.get(10)
            ),
            MapOperation.put(
                MapPolicy.Default,
                "test_bin",
                Value.get(4),
                Value.get(20)
            ),
            MapOperation.put(
                MapPolicy.Default,
                "test_bin",
                Value.get(3),
                Value.get(30)
            ),
            MapOperation.put(
                MapPolicy.Default,
                "test_bin",
                Value.get(5),
                Value.get(40)
            ),
            MapOperation.put(
                MapPolicy.Default,
                "test_bin",
                Value.get(2),
                Value.get(50)
            ),
            MapOperation.removeByIndexRange("test_bin", -3, MapReturnType.INVERTED)
            // MapOperation.getByIndexRange(
            //     "test_bin",
            //     -3, 3,
            //     MapReturnType.ORDERED_MAP
                
            // )
        );
        client.close(); 
    }
}