package com.mycompany.app;

import com.aerospike.client.Host;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.useServicesAlternate = true;

        // Write policy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        
        // Read policy
        Policy policy = new Policy();
        policy.socketTimeout = 300;
    
        // Update policy
        WritePolicy updatePolicy = new WritePolicy();
        updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        AerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3104);

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
        
        // Record exists
        boolean exists = client.exists(policy, key);
        System.out.format("Exists: %s\n", exists);

        // Get record metadata
        Record record_header = client.getHeader(policy, key);
        System.out.format("Record metadata: %s\n", record_header);

        // Get whole record
        Record record = client.get(policy, key);
        System.out.format("Whole record, bins: %s\n", record.bins);

        // Update record
        Bin newPosted = new Bin("reported", 20240602);
        client.put(updatePolicy, key, newPosted);
        Record record2 = client.get(policy, key);
        System.out.format("Whole record, bins: %s\n", record2.bins);

        // // Delete record using client.put
        // Bin removeBin = Bin.asNull("report");
        // client.put(null, key, removeBin);
        // Record record3 = client.get(policy, key);
        // System.out.format("Whole record, bins : %s\n", record3.bins);

        // // Delete record using client.delete
        // WritePolicy deletePolicy = new WritePolicy();
        // deletePolicy.durableDelete = true;
        // client.delete(null, key);
        // boolean exists2 = client.exists(policy, key);
        // System.out.format("Exists: %s \n", exists2);
        // Single operation. Create KEY_ORDERED map policy
        
        MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        client.operate(null, key, MapOperation.setMapPolicy(mapPolicy, "report"));
        
        // Multiple operations.
        System.out.println("Multiple operations"); 
        Bin posted2 = new Bin("posted", 20220602);
        Record record4 = client.operate(null, key, 
            Operation.put(posted2), 
            MapOperation.put(MapPolicy.Default, "report", Value.get("city"), Value.get("Ypsilanti")), 
            Operation.get("report")
        );
        System.out.format("Record : %s\n", record4.bins);

        // Filter Read
        System.out.println("Filter Read:");
        Policy filter_read_policy = new Policy();
        filter_read_policy.filterExp = Exp.build(
            Exp.gt(
                ListExp.size(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, Exp.val("shape"), Exp.mapBin("report"))
                ),
                Exp.val(2)
            )
        );
        Record filterReadRecord = client.get(filter_read_policy, key);
        System.out.format("Record %s\n", filterReadRecord.bins);

        // Filter Write
        WritePolicy filterWritePolicy = new WritePolicy();
        filterWritePolicy.filterExp = Exp.build(
            Exp.and(
                Exp.gt(Exp.intBin("occured"), Exp.val(20211231)),
                Exp.binExists("posted")
            )
        );
        client.operate(filterWritePolicy, key, MapOperation.put(MapPolicy.Default, "report", Value.get("recent"), Value.get(true)));

        // Operation Expression Read
        Expression operationReadExp = Exp.build(
            ListExp.size(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, Exp.val("shape"), Exp.mapBin("report"))
            )
        );
        Record operationExpReadRecord = client.operate(null, key, 
            ExpOperation.read("numShapes", operationReadExp, ExpReadFlags.DEFAULT)
        );
        System.out.format("Record: %s\n", operationExpReadRecord.bins);

        // Operation Expression Write
        Expression exp = Exp.build(
            MapExp.put(MapPolicy.Default, Exp.val("recent"), 
                Exp.and(
                    Exp.gt(Exp.intBin("occurred"), Exp.val(20211231)),
                    Exp.binExists("posted")
                ),
                Exp.mapBin("report")
            )
        );
        client.operate(null, key, ExpOperation.write("report", exp, ExpWriteFlags.DEFAULT));

        // Close connection
        client.close();
    }
}
