# FIPS Compliance

We tested FIPS compliance for the client using OpenSSL 3.0. To use the client in FIPS-compliant mode, use OpenSSL 3.0. Older versions of OpenSSL have not been verified (although they may work).

## Communication between client and Kafka cluster

### Installing client using OpenSSL and librdkafka bundled in wheels

If you install this client through prebuilt wheels using `pip install confluent_kafka`, OpenSSL 3.0 is already statically linked with the librdkafka shared library. To enable this client to communicate with the Kafka cluster using the OpenSSL FIPS provider and FIPS-approved algorithms, you must enable the FIPS provider. You can find steps to enable the FIPS provider in section [Enabling FIPS provider](#enabling-fips-provider).

You should follow the same above steps if you install this client from the source using `pip install confluent_kafka --no-binary :all:` with prebuilt librdkafka in which OpenSSL is statically linked


### Installing client using system OpenSSL with librdkafka shared library

When you build the librdkafka from source, librdkafka dynamically links to the OpenSSL present in the system (if static linking is not used explicitly while building). If the installed OpenSSL is already working in FIPS mode, then you can directly jump to the section [Client configuration to enable FIPS provider](#client-configuration-to-enable-fips-provider) and enable `fips` provider. If you don't have OpenSSL working in FIPS mode, use the steps mentioned in the section [Enabling FIPS provider](#enabling-fips-provider) to make OpenSSL in your system FIPS compliant and then enable the `fips` provider. Once you have OpenSSL working in FIPS mode and the `fips` provider enabled, librdkafka and python client will use FIPS approved algorithms for the communication between client and Kafka Cluster using the producer, consumer or admin client.

### Enabling FIPS provider

To enable the FIPS provider, you must have the FIPS module available on your system, plug the module into OpenSSL, and then configure OpenSSL to use the module. 
You can plug the FIPS provider into OpenSSL two ways: 1) put the module in the default module folder of OpenSSL or 2) point to the module with the environment variable, `OPENSSL_MODULES`. For example: `OPENSSL_MODULES="/path/to/fips/module/lib/folder/`

You configure OpenSSL to use the FIPS provider using the FIPS configuration in OpenSSL config. You can 1) modify the default configuration file to include FIPS related config or 2) create a new configuration file and point to it using the environment variable,`OPENSSL_CONF`. For example `OPENSSL_CONF="/path/to/fips/enabled/openssl/config/openssl.cnf` For an example of OpenSSL config, see below.

**NOTE:** You need to specify both `OPENSSL_MODULES` and `OPENSSL_CONF` environment variable when installing the client from pre-built wheels or when OpenSSL is statically linked to librdkafka.

#### Steps to build FIPS provider module

You can find steps to generate the FIPS provider module in the [README-FIPS doc](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)

In short, you need to perform the following steps:

1) Clone OpenSSL from [OpenSSL Github Repo](https://github.com/openssl/openssl)
2) Checkout the correct version. (v3.0.8 is the current FIPS compliant version for OpenSSL 3.0 at the time of writing this doc.)
3) Run `./Configure enable-fips`
4) Run `make install_fips`

After last step, two files are generated.
* FIPS module (`fips.dylib` in Mac, `fips.so` in Linux and `fips.dll` in Windows)
* FIPS config (`fipsmodule.cnf`) file will be generated. Which needs to be used with OpenSSL.

#### Referencing FIPS provider in OpenSSL

As mentioned earlier, you can dynamically plug the FIPS module built above into OpenSSL by putting the FIPS module into the default OpenSSL module folder (only when installing from source). For default locations of OpenSSL on various operating systems, see the SSL section of the [Introduction to librdkafka - the Apache Kafka C/C++ client library](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl). It should look something like `...lib/ossl-modules/`.

You can also point to this module with the environment variable `OPENSSL_MODULES`. For example, `OPENSSL_MODULES="/path/to/fips/module/lib/folder/`

#### Linking FIPS provider with OpenSSL

To enable FIPS in OpenSSL, you must include `fipsmodule.cnf` in the file, `openssl.cnf`. The `fipsmodule.cnf` file includes `fips_sect` which OpenSSL requires to enable FIPS. See the example below. 

Some of the algorithms might have different implementation in FIPS or other providers. If you load two different providers like default and fips, any implementation could be used. To make sure you fetch only FIPS compliant version of the algorithm, use `fips=yes` default property in config file.

OpenSSL config should look something like after applying the above changes.

```
config_diagnostics = 1
openssl_conf = openssl_init

.include /usr/local/ssl/fipsmodule.cnf

[openssl_init]
providers = provider_sect
alg_section = algorithm_sect

[provider_sect]
fips = fips_sect

[algorithm_sect]
default_properties = fips=yes
.
.
.
```

### Client configuration to enable FIPS provider

OpenSSL requires some non-crypto algorithms as well. These algorithms are not included in the FIPS provider and you need to use the `base` provider in conjunction with the `fips` provider. Base provider comes with OpenSSL by default. You must enable `base` provider in the client configuration.

To make client (consumer, producer or admin client) FIPS compliant, you must enable only `fips` and `base` provider in the client using the `ssl.providers` configuration property i.e `'ssl.providers': 'fips,base'`.


## Communication between client and Schema Registry

The communication between client and Schema Registry is also FIPS compliant if the underlying python is using FIPS compliant OpenSSL. This depends on the system level OpenSSL if the python is installed in default way. To know more on how to use FIPS provider with OpenSSL, check [How to use FIPS Module](https://www.openssl.org/docs/man3.0/man7/fips_module.html) and [Generating FIPS module and config file](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md) links. The steps are briefly described above as well.

## References
* [Generating FIPS module and config file](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)
* [How to use FIPS Module](https://www.openssl.org/docs/man3.0/man7/fips_module.html)
* [librdkafka SSL Information](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl)
