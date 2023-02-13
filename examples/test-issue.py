import confluent_kafka

config={
    "bootstrap.servers": "my-kafka-broker.local:8443",
    "security.protocol": "SSL",
    "ssl.ca.location": "/etc/pki/tls/certs/ca-bundle.crt",
    "ssl.certificate.location": "me.crt",
    "ssl.key.location": "me.key",
}
p = confluent_kafka.Producer(config)

print(p.list_topics().brokers)