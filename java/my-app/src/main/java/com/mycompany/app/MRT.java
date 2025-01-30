package com.mycompany.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/* Record
*/

public class MRT {
    
    public void runTest() throws AerospikeException {
        ClientPolicy policy = new ClientPolicy();
        policy.setUseServicesAlternate(true);
        AerospikeClient client = new AerospikeClient(policy, "127.0.0.1", 3100);

        String namespace = "test";
        String set = "ufo";
        String binName = "sighting";
        int startIndexOffset = 5001;

        // Example JSON string
        String sightings =
        "[{\"sighting\":{\"occurred\":20200912,\"reported\":20200916,\"posted\":20201105,\"report\":{\"city\":\"Kirkland\",\"duration\":\"~30 minutes\",\"shape\":[\"circle\"],\"state\":\"WA\",\"summary\":\"4 rotating orange lights in the Kingsgate area above the Safeway. Around 9pm the power went out in the Kingsgate area.  Four lights were spotted rotating above the local Safeway and surrounding streets.  They were rotating fast but staying relatively in the same spots.  Also described as orange lights. About thirty minutes later they disappeared.  The second they disappeared the power was restored.  Later a station of police from Woodinville and Kirkland came to guard the street where it happened.  They wouldn't let anyone go past the street, putting out search lights and flare signals so people couldn't drive past Safeway.  The police also would not let people walk past to go home.\"},\"location\":\"\\\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[-122.1966441,47.69328259]}\\\"\"}},\n" +
        "{\"sighting\":{\"occurred\":20200322,\"reported\":20200322,\"posted\":20200515,\"report\":{\"city\":\"Pismo Beach\",\"duration\":\"5 minutes\",\"shape\":[\"light\"],\"state\":\"CA\",\"summary\":\"About 20 solid, bright lights moving at the same altitude, heading and speed.  Spaced perfectly apart flying over the ocean headed south.\"},\"location\":\"\\\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[-120.6595,35.1546]}\\\"\"}},\n" +
        "{\"sighting\":{\"occurred\":20200530,\"reported\":20200531,\"posted\":20200625,\"report\":{\"city\":\"New York Staten Island\",\"duration\":\"2 minutes\",\"shape\":[\"disk\"],\"state\":\"NY\",\"summary\":\"Round shaped object observed over Staten Island NYC, while sitting in my back yard. My daughter also observed this object . Bright White shaped object moving fast from East to West . Observed over Graniteville, Staten Island towards the Elizabeth NJ area and appears to be fast. We then lost view of it due to the clouds.\"}}}]";

        // Convert string to Java Map
        List<Map<String, Object>> sightingMap = new Gson().fromJson(sightings, new TypeToken<ArrayList<HashMap<String, Object>>>(){}.getType());
        // List of keyIds starting with "startIndexOffset"
        List<Integer> keyIds = IntStream.range(startIndexOffset, sightingMap.size() + startIndexOffset).boxed().toList();

        // Create Write Policy
        WritePolicy writePolicy = client.copyWritePolicyDefault();
        // Start transaction
        Txn writeTransaction = new Txn();
        System.out.printf("Begin transaction: %d", writeTransaction.getId());
        for (Integer keyId : keyIds) {
            // Define Aerospike key
            Key key = new Key(namespace, set, keyId.intValue());
            writePolicy.txn = writeTransaction;
            // Extracting value from record for secondary index column
            Map<String, Object> entry =  sightingMap.get(keyId.intValue() % startIndexOffset);
            Map<String, Map<String, Object>> value = (Map<String, Map<String, Object>>) entry.get("sighting");
            // Set bins for "sightings" and "city"
            Bin sightingsBin = new Bin(binName, Value.get(entry));

            // Writing record to Aerospike
            try {
                client.put(writePolicy, key, sightingsBin);
                System.out.printf("Create succeeded \nKey: %d \nRecord: %s", key.userKey.toLong(), new Gson().toJson(value));
            } // Write failed
            catch (AerospikeException ae) {
                // Aborting transaction on failure
                client.abort(writeTransaction);
                System.out.println("Write failed\\nError: " + ae.getMessage());
            }
        }
        // Commit successful transaction
        System.out.printf("Commit transaction: %d", writeTransaction.getId());
        client.commit(writeTransaction);
        client.close();
    }
    public static void main(String[] args) {
        try {
            MRT mrt = new MRT();
            mrt.runTest();
        } catch (AerospikeException e) {
            e.printStackTrace();
        }
    }
}