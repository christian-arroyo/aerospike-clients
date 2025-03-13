package com.mycompany.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;

public class Tls {
    public static void main(String[] args) {
        // Create Client Policy
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;
        clientPolicy.tlsPolicy = new TlsPolicy();

        // Read policy
        Policy readPolicy = new Policy();
        readPolicy.socketTimeout = 300;

        // Write policy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;

        Host[] hosts = new Host[] {
            new Host("127.0.0.1", "tls1", 3102)
        };

        AerospikeClient client = new AerospikeClient(clientPolicy, hosts);

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

        // Get record metadata
        Record record_header = client.getHeader(readPolicy, key);
        System.out.format("Record metadata: %s\n", record_header);

        // Get whole record
        Record record = client.get(readPolicy, key);
        System.out.format("Whole record, bins: %s\n", record.bins);

        client.close();
    }
}
