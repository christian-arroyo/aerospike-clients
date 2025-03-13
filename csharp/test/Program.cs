// using Aerospike.Client;

// Host config = new Host("127.0.0.1", 3100);
// ClientPolicy clientPolicy = new ClientPolicy();
// clientPolicy.useServicesAlternate = true;
// AerospikeClient client = new AerospikeClient(clientPolicy, config);

// Policy policy = new Policy();
// policy.socketTimeout = 0;

// Statement statement = new Statement();
// statement.SetNamespace("test");
// statement.SetSetName("asbench");
// statement.MaxRecords = 2;
// statement.SetBinNames("testbin");

// RecordSet rs = client.Query(null, statement);

// while (rs.Next()) {
//     var key = rs.Key;
//     var record = rs.Record;
//     Console.WriteLine("Key: {0}", key.userKey);
//     foreach (var group in record.bins)
//     {
//         Console.WriteLine("Bins: {0}", group);
//     }
// }
// rs.Close();
// client.Close();

using Aerospike.Client;

var address = "localhost";
int port = 3102;
var tlsName = "tls1";
var tlsPolicy = new TlsPolicy("", "", "/Users/carroyo/cacert.pem", false);
var policy = new ClientPolicy();
policy.tlsPolicy = tlsPolicy;
policy.useServicesAlternate = true;
AerospikeClient client = new AerospikeClient(policy, new Host(address, tlsName, port));
client.Close();

// Statement statement = new Statement();
// statement.SetNamespace("test");
// statement.SetSetName("asbench");
// statement.MaxRecords = 2;
// statement.SetBinNames("testbin");

// RecordSet rs = client.Query(null, statement);

// while (rs.Next()) {
//     var key = rs.Key;
//     var record = rs.Record;
//     Console.WriteLine("Key: {0}", key.userKey);
//     foreach (var group in record.bins)
//     {
//         Console.WriteLine("Bins: {0}", group);
//     }
// }
// rs.Close();
client.Close();
