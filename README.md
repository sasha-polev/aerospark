Aerospike Spark Connector
===============

NOTE: this version requires Spark 1.2.0+, if you are using Spark 1.1.x please use [Spark-1.1.0](/sasha-polev/aerospark/tree/Spark-1.1.0) tag
--------------------------------------------------------------------------------------------------

Spark glue to efficiently read data from Aerospike

  * Creates schema RDD from AQL statement (including determining datatypes on the single row query) or just RDD[Row] if not to be used with SparkSQL context
  * Queries local Aerospike nodes in parallel (allows parrallel reads from single server for range queries)
  
Example use:

```
import com.osscube.spark.aerospike.rdd._;import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
val aero  = sc.aeroSInput(("192.168.142.162" , 3000), "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000", sqlContext ,6)
aero.registerTempTable("aero")
sqlContext.sql("select avg(intColumn1) from aero where intColumn1 < 0").collect
```

(Assumes there is an numeric index on intColumn1, creates 6 partitions per server)

Other way to create and SQL RDD is to use  method on SQLContext itself:

```
import com.osscube.spark.aerospike.rdd._;import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
val aero = sqlContext.aeroRDD(("192.168.142.162",3000), "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000")
aero.count
```

Spark SQL use
-------------

New Spark 1.2.0 Data Sources API allows integration with Spark SQL CLI and Thrift/JDBC server:

Run SQL CLI or server with `--jars` pointing to the library (similar to Spark CLI):

```
spark-sql --jars  <path to build>/aerospike-spark-0.2-SNAPSHOT-jar-with-dependencies.jar
```

Then you can use statements like the following:


```
CREATE TEMPORARY TABLE aero
USING com.osscube.spark.aerospike.rdd
OPTIONS (initialHost "192.168.142.162:3000",
select "select column1,column2,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000",
partitionsPerServer "2");
```

or

```
CREATE TEMPORARY TABLE aero
USING com.osscube.spark.aerospike.rdd
OPTIONS (initialHost "192.168.142.162:3000",
select "select * from test.one_million");
```

When you do subsequent selects best efforts will be made to push down at least (and at most) one predicate to Aerospike, if index is present.

```
select count(distinct column1) from aero where column1  = 'G';
```

or

```
select count(distinct column2) from aero where  intColumn1  > -1000000 and intColumn1 < 100000;
```
