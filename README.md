# Aerospike Spark Connector
The Aerospike spark connector provides features to represent data stored in Aerospike as a DataFrame in Spark.
 
Aerospike Spark Connector includes:
- Reading from Aerospike to a DataFrame
- Saving a DataFrame to Aerospike
- Spark SQL multiple filters pushed down to the Aerospike cluster

## How to build

The source code for this solution is available on GitHub at https://github.com/citrusleaf/aerospark/ 


This Library requires Java JDK 7+ Scala 2.10, SBT 0.13 and the `aerospike-helper-java-<version>.jar` 

Note that this will build the JAR `aerospike-helper-java-<version>.jar` and install it in your local Maven repository. 

Clone the [Aerospike Spark](https://github.com/citrusleaf/aerospark/.git) repository using this command:
```bash
$ git clone https://github.com/citrusleaf/aerospark/.git
```
Aerospike Spark depends on the [Aerospike Helper](https://github.com/aerospike-labs/aerospike-helper.git) project. Before you can build Aerospike JDBC you will need to add it's GitHub repository as a submodule. 

Update the submodule dependency for `aerospike-helper`:
```bash
$ git submodule update --init
```
Build the [Aerospike Helper](https://github.com/aerospike-labs/aerospike-helper.git) this command:
```bash
$ mvn clean install -DskipTests
```
To build [Aerospike Spark](https://github.com/citrusleaf/aerospark/.git) use this command:
```bash
$ sbt assembly
```
A JAR file will be produced in the `target` directory : `aerospike-jdbc-<version>.jar`. This JAR contains the JDBC driver and all the dependencies.

## Loading and Saving DataFrames 
