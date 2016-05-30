# Aerospike Spark Connector
The Aerospike spark connector provides features to represent data stored in Aerospike as a DataFrame in Spark.
 
Aerospike Spark Connector includes:
- Reading from Aerospike to a DataFrame
- Saving a DataFrame to Aerospike
- Spark SQL multiple filters pushed down to the Aerospike cluster

## How to build

The source code for this solution is available on GitHub at [https://github.com/citrusleaf/aerospark](https://github.com/citrusleaf/aerospark). SBT is the build tool and it will create a Uber (fat) jar as the final output of the build process. The jar will contain all the class files and dependencies.

This Library requires Java JDK 7+ Scala 2.10, SBT 0.13, Maven and the `aerospike-helper-java` 

Before you build the Aerospike spark connector you need to build the `aerospike-helper-java.jar` and install it in your local maven repository. The `aerospike-helper-java.jar` is used by the connector to perform efficent, multi-filter queries on Aerospike.

Clone the [Aerospike Helper](https://github.com/aerospike/aerospike-helper) repository using this command:
```bash
$ git clone https://github.com/aerospike/aerospike-helper
```
Navigate to the subdirectory `java` and run the following command to build and install the helper class jar:
```bash
$ mvn clean install -DskipTests
```
When maven is complete the `aerospike-helper-java.jar` will be installed in your local maven repository

To build the Spark connector:
Clone the [Aerospike Spark](https://github.com/citrusleaf/aerospark/.git) repository using this command:
```bash
$ git clone https://github.com/citrusleaf/aerospark/.git
```
After cloning the repository, build the uber jar using:
```bash
$ sbt assembly
```
Note that during the build, a number of unit tests are run, these tests will assume an Aerospike cluster is running at "127.0.0.1" on port 3000. If you want to ignote the unit tests, use:
```bash
sbt 'set test in assembly := {}' clean assembly
```

On conclustion of the build, the uber JAR `some jar name` will be located in the subdirectory `some place`.
## Loading and Saving DataFrames 
