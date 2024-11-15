package com.mycompany.app;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        // Establish a connection to the server
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3100);

        // Write policy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;

        Key key = new Key("test", "test", 123456789);
        
        // Create a list of shapes to add to the report map
        ArrayList<String> shape = new ArrayList<String>();
        shape.add("circle");
        shape.add("flash");
        shape.add("disc");

        // Create a report map
        Map reportMap = new HashMap<String, Object>();
        reportMap.put("city", "Ann Arbor");
        reportMap.put("state", "Michigan");
        reportMap.put("shape", shape);
        reportMap.put("duration", "5 minutes");
        reportMap.put("summary", "Large flying disc flashed in the sky above the student union. Craziest thing I've ever seen!");

        // Format coordinates as a GeoJSON string
        String geoLoc = "{\"type\":\"Point\", \"coordinates\":[42.2808,83.7430]}";

        // Create the bins as Bin("binName", value)
        // Create the bins as Bin("binName", value)
        Bin occurred = new Bin("occurred", 20220531);
        Bin reported = new Bin("reported", 20220601);
        Bin posted = new Bin("posted", 20220601);
        // reportMap defined in the section above
        Bin report = new Bin("report", reportMap); 
        // geoLoc defined in the section above
        Bin location = new Bin("location", Value.getAsGeoJSON(geoLoc));

        // Write the record to Aerospike
        client.put(writePolicy, key, occurred, reported, posted, report, location);
        client.close();
    }
}
