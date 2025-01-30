import aerospike
import sys

aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG) 

tls_ip = "127.0.0.1"
tls_port = 3102
tls_name = "tls1"
tls_host_tuple = (tls_ip, tls_port, tls_name)
hosts = [tls_host_tuple]

tls_config = {
    "cafile": "/Users/carroyo/cacert.pem",
    "enable": True
}

try:
    client = aerospike.client({
        "hosts": hosts,
        "use_services_alternate": True,
        "tls": tls_config
    })
    print("Successfully connected")
    client.close();
    
except Exception as e:
    print(e)
    print("Failed to connect")
    sys.exit()
