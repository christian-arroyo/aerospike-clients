#!/usr/bin/ruby
require 'rubygems'
require 'aerospike'
require 'io/console'

include Aerospike

policy = { connection_queue_size: 64, timeout: 1.000, tend_interval: 10000 }
client = Aerospike::Client.new("10.128.0.22:3000", policy: policy)

key = Key.new("test", "demo", 123)

bins = {
    "a" => "Lack of skill dictates economy of style.",
    "b" => 123,
    "c" => [1, 2, 3],
    "d" => {"a" => 42, "b" => "An elephant is mouse with an operating system."},
}

begin
    r0 = client.put(key, bins)
    sleep 15
    # disconnect aerospike instance (killed asd process)
    # docker network disconnect castle_default castle-aerospike-1
    r1 = client.get(Aerospike::Key.new("castle-dev", "fp", "test"))
    puts r1
  rescue Aerospike::Exceptions::Timeout => e
    puts e # #<Aerospike::Exceptions::Timeout: Timeout after 4 attempts!>
    puts e.failed_nodes # nil
    puts e.failed_connections # nil
    puts e.inspect # #<Aerospike::Exceptions::Timeout: Timeout after 4 attempts!>
    raise
  end

client.close