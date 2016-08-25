# presto-hyperloglog
Algebird's HyperLogLog support for Facebook Presto (prestodb.io).

Functions
-------------
There are two types of functions, aggregation and scalar.

### Aggregation
`merge(<hll: HLL>)` -> HLL

### Scalar
`create_hll(<element:VARCHAR>, <bits: BIGINT>)` -> HLL

`cardinality(<hll: HLL>)` -> BIGINT

Building
-------------
As this is a Presto plugin it relies on presto-spi. This means you will have to build Presto first and this project expects to
find Presto as a parent project.

/presto/

/presto/presto-main

/presto/presto-hyperloglog

Then run `mvn clean install` in /presto. Once that has finished run `mvn clean install` in /presto/presto-hyperloglog.
Finally add the plugin to `/presto/presto-main/etc/config.properties`.

Deployment on AWS EMR
---------------------
The uber jar is now built by maven.

```bash
mkdir /usr/lib/presto/plugin/presto-hyperloglog
cd /usr/lib/presto/plugin/presto-hyperloglog
sudo wget https://github.com/vitillo/presto-hyperloglog/raw/master/target/presto-hyperloglog-$PRESTO_VERSION-jar-with-dependencies.jar
```
