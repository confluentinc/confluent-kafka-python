Use `generate_certificates.sh` in secrets folder to generate the certificates.
Up the server using `docker-compose up`.
Use example producer and consumer to test the FIPS compliance. Note that you might need to point to FIPS module and FIPS enabled OpenSSL 3.0 config using environment variables like ` OPENSSL_CONF="/path/to/fips/enabled/openssl/config/openssl.cnf" OPENSSL_MODULES="/path/to/fips/module/lib/folder/" ./examples/fips/fips_producer.py localhost:9092 test-topic`

Uncomment `KAFKA_SSL_CIPHER.SUITES: TLS_CHACHA20_POLY1305_SHA256` in `docker-compose.yml` to enable non FIPS compliant algorithm. Use this to verify that only FIPS compliant algorithms are used.