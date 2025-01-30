<?php
namespace Aerospike;

$socket = "/tmp/asld_grpc.sock";

try {
    $client = Client::connect($socket);
    echo "* Connected to the local daemon: $client->socket \n"; 
} catch (\Throwable $th) {
    echo "Failed connecting to Aerospike Server with Exception: ".$e;
}

// // Instantiate the WritePolicy object
// $writePolicy = new WritePolicy();

// $writePolicy->setRecordExistsAction(RecordExistsAction::Update);
// $writePolicy->setGenerationPolicy(GenerationPolicy::ExpectGenEqual);
// $writePolicy->setExpiration(Expiration::seconds(3600)); // Expiring in 1 hour
// $writePolicy->setMaxRetries(3);
// $writePolicy->setSocketTimeout(5000);