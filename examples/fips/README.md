# FIPS Compliance

We tested FIPS compliance for the client using OpenSSL 3.0. To use the client in FIPS-compliant mode, use OpenSSL 3.0. Older versions of OpenSSL have not been verified (although they may work).

## Communication between client and Kafka cluster

### Install from prebuilt wheels

If you install the this client through prebuilt wheels using `pip`, it comes with librdkafka in which OpenSSL 3.0 is already statically linked. To enable this client to communicate with the Kafka cluster using the OpenSSL FIPS provider and FIPS-approved algorithms, you must enable the FIPS provider. You can find steps to enable the FIPS provider in section [Enabling FIPS provider](#enabling-fips-provider).


### Install from source by building librdkafka

When building librdkafka and python client from source, librdkafka uses OpenSSL present in the system by dynamically linking it. If the installed OpenSSL is already working in FIPS mode, then you can directly jump to section [Client configuration to enable FIPS provider](#client-configuration-to-enable-fips-provider), otherwise use steps mentioned in section [Enabling FIPS provider](#enabling-fips-provider) to make system OpenSSL FIPS compliant. Once that is done, librdkafka and python client will use FIPS approved algorithms for the communication between client and Kafka Cluster using producer, consumer or admin client.

### Enabling FIPS provider

We assume that you have FIPS provider (module) available in your system. This module can be dynamically plugged into OpenSSL by putting this module in default module folder of OpenSSL or pointing to this module with environment vairable `OPENSSL_MODULES` (folder containing the FIPS module in the form of dynamic library). We also need to use OpenSSL config which has FIPS configuration present in it. Default configuration file can be modified to include FIPS related config or a new configuration file can also be pointed using `OPENSSL_CONF` environment variable.

#### Steps to generate FIPS provider

You can find steps to generate the FIPS provider in the [README-FIPS doc](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)

In short, you need to perform the following steps:

1) Clone OpenSSL from [OpenSSL Github Repo](https://github.com/openssl/openssl)
2) Checkout the correct version. (v3.0.8 is the current FIPS compliant version for OpenSSL 3.0 at the time of writing this doc.)
3) Run `./configure --enable-fips`
4) Run `make install_fips`

After last step, two files are generated.
* FIPS module (`fips.dylib` in Mac, `fips.so` in Linux and `fips.dll` in Windows)
* FIPS config (`fipsmodule.cnf`) file will be generated. Which needs to be used with OpenSSL.

#### Referencing FIPS provider in OpenSSL

As mentioned earlier, the above generated FIPS module can be dynamically plugged into OpenSSL by putting this module in default module folder (Refer [this](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl) for typical default locations on various OS) of OpenSSL. It should look something like `...lib/ossl-modules/`.

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

This part is not tested for FIPS compliance right now.

## References
* [Generating FIPS module and config file](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)
* [How to use FIPS Module](https://www.openssl.org/docs/man3.0/man7/fips_module.html)
* [librdkafka SSL Information](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl)
