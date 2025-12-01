#!/bin/bash
set -e
server_hostname=${server_hostname:-localhost}
client_hostname=${client_hostname:-localhost}
cert_password=${cert_password:-111111}
openssl=${openssl:-openssl}
keytool=${keytool:-keytool}
echo openssl version
$openssl version
echo creates server keystore
$keytool -keystore server.keystore.jks -storepass $cert_password -alias ${server_hostname} -validity 365 -genkey -keyalg RSA -dname "cn=$server_hostname"
echo creates root CA
$openssl req -nodes -new -x509 -keyout ca-root.key -out ca-root.crt -days 365 -subj "/C=US/ST=CA/L=MV/O=CFLT/CN=CFLT"
echo creates CSR
$keytool -keystore server.keystore.jks -alias ${server_hostname} -certreq -file ${server_hostname}_server.csr -storepass $cert_password
echo sign CSR
$openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in ${server_hostname}_server.csr -out ${server_hostname}_server.crt -days 365 -CAcreateserial
echo import root CA
$keytool -keystore server.keystore.jks -alias CARoot -import -noprompt -file ca-root.crt -storepass $cert_password
echo import server certificate
$keytool -keystore server.keystore.jks -alias ${server_hostname} -import -file ${server_hostname}_server.crt -storepass $cert_password
echo create client CSR
$openssl req -newkey rsa:2048 -nodes -keyout ${client_hostname}_client.key -out ${client_hostname}_client.csr -subj "/C=US/ST=CA/L=MV/O=CFLT/CN=CFLT" -passin pass:$cert_password
echo sign client CSR
$openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in ${client_hostname}_client.csr -out ${client_hostname}_client.crt -days 365 -CAcreateserial
echo create client keystore
$openssl pkcs12 -export -in ${client_hostname}_client.crt -inkey ${client_hostname}_client.key -name ${client_hostname} -out client.keystore.p12 -passin pass:$cert_password \
-passout pass:$cert_password
echo create truststore
$keytool -noprompt -keystore server.truststore.jks -alias CARoot -import -file ca-root.crt -storepass $cert_password
echo create creds file
echo "$cert_password" > ./creds
echo verify with: openssl pkcs12 -info -nodes -in client.keystore.p12
