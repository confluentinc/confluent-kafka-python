# FIPS Compliance

## Communication between client and Kafka cluster

### Install from prebuilt wheels

If this client is installed using `pip`, it comes with librdkafka in which OpenSSL 3.0 is statically linked. This client can communicates with Kafka cluster with FIPS approved algorithms using FIPS provider provided by OpenSSL.


### Install from source by building librdkafka

When building librdkafka and python client from source, librdkafka uses OpenSSL present in the system by dynamic linking it. If the installed OpenSSL is already working in FIPS mode, then you can directly jump to section [Client configuration to enable fips provider](#client-configuration-to-enable-fips-provider), otherwise use below steps to make system OpenSSL FIPS compliance. Once that is done, librdkafka and python client will use FIPS approved algorithms for the communication between client and Kafka Cluster using producer, consumer or admin client.

### Enabling FIPS provider

We assume that you have FIPS provider (module) available in your system. This module can be dynamically plugged into OpenSSL by putting this module in default module folder of OpenSSL or pointing this module with environment vairable `OPENSSL_MODULES` (folder containing the fips module in the form of dynamic library). We also need to use OpenSSL config which has FIPS configuration present in it. Default configuration file can be modified to include FIPS related config or a new configuration file can also be pointed using `OPENSSL_CONF` environment variable.

#### Steps to generate FIPS provider

Steps to generate fips provider can be found on the [README-FIPS doc](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)

In short, you need to perform the following steps:

1) Clone OpenSSL from [OpenSSL Github Repo](https://github.com/openssl/openssl)
2) Checkout the correct version. (v3.0.8 is the current fips compliant version for OpenSSL 3.0 at the time of writing this doc.)
3) Run `./configure --enable-fips`
4) Run `make install_fips`

After last step, two files are generated.
* fips module (`fips.dylib` in Mac, `fips.so` in Linux and `fips.dll` in Windows)
* fips config (`fipsmodule.cnf`) file will be generated. Which needs to be used with OpenSSL.

#### Referencing FIPS provider in OpenSSL

As mentioned earliar, the above generated fips module can be dynamically plugged into OpenSSL by putting this module in default module folder (Refer [this](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl) for typical default locations on various OS) of OpenSSL. It should look something like `...lib/ossl-modules/`.

This module can also be pointed with environment vairable `OPENSSL_MODULES` (folder containing the fips module in the form of dynamic library)

#### Linking FIPS provider with OpenSSL

`fipsmodule.cnf` file includes `fips_sect` which is required to enable fips in OpenSSL. `fipsmodule.cnf` needs to be included in `openssl.cnf` file to make it work.

Some of the algorithms might have different implementation in fips or other providers. If you load two different providers like default and fips, any implementation could be used. To make sure the we fetch only fips compliant version of the algorithm, we use `fips=yes` default property in config file.

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

### Client configuration to enable fips provider

Some of the non crypto algorithms are required in OpenSSL. These algorithms are not included in fips provider and hence we need to use `base` provider in conjunction with `fips` provider. Base provider comes with OpenSSL by default. We just need to enable it in the client configuration.

To make client (consumer, producer or admin client) fips compliant, we need to enable only `fips` and `base` provider in the client using `ssl.providers: 'fips,base'` property.


## Communication between client and Schema Registry

This part is not tested for FIPS compliance right now.

## References
* [Generating FIPS module and config file](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)
* [How to use FIPS Module](https://www.openssl.org/docs/man3.0/man7/fips_module.html)
* [librdkafka SSL Information](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl)
