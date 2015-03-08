Aerospike Spark Connector
===============

NOTE: this version requires Spark 1.2.0+, if you are using Spark 1.1.x please use [Spark-1.1.0](https://github.com/sasha-polev/aerospark/tree/Spark-1.1.0) tag
--------------------------------------------------------------------------------------------------

Spark glue to efficiently read data from Aerospike

  * Creates schema RDD from AQL statement (including determining datatypes on the single row query) or just RDD[Row] if not to be used with SparkSQL context
  * Queries local Aerospike nodes in parallel (allows parrallel reads from single server for range queries)
  
Example use:

```sql
import com.osscube.spark.aerospike.rdd._;import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
val aero  = sc.aeroSInput("192.168.142.162:3000",
 "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000", sqlContext ,6)
aero.registerTempTable("aero")
sqlContext.sql("select avg(intColumn1) from aero where intColumn1 < 0").collect
```

(Assumes there is an numeric index on intColumn1, creates 6 partitions per server)

Other way to create and SQL RDD is to use  method on SQLContext itself:

```sql
import com.osscube.spark.aerospike.rdd._;import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
val aero = sqlContext.aeroRDD("192.168.142.162:3000",
 "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000")
aero.count
```

Spark SQL use
-------------

New Spark 1.2.+ Data Sources API allows integration with Spark SQL CLI and Thrift/JDBC server:

Run SQL CLI or server with `--jars` pointing to the library (similar to Spark CLI):

```
spark-sql --jars  <path to build>/aerospike-spark-0.2-SNAPSHOT-jar-with-dependencies.jar
```

Then you can use statements like the following:


```sql
CREATE TEMPORARY TABLE aero
USING com.osscube.spark.aerospike.rdd
OPTIONS (initialHost "192.168.142.162:3000",
select "select column1,column2,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000",
partitionsPerServer "2");
```

or

```sql
CREATE TEMPORARY TABLE aero
USING com.osscube.spark.aerospike.rdd
OPTIONS (initialHost "192.168.142.162:3000",
select "select * from test.one_million");
```

When you do subsequent selects best efforts will be made to push down at least (and at most) one predicate to Aerospike, if index is present.

```sql
select count(distinct column1) from aero where column1  = 'G';
```

or

```sql
select count(distinct column2) from aero where  intColumn1  > -1000000 and intColumn1 < 100000;
```

This version is tested with Aerospike 3.5 and has an optional parameter useUdfWithoutIndexQuery to allow UDF filtering even if no index is used in a query, e.x:

```sql
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
sqlContext.sql("CREATE TEMPORARY TABLE aero USING com.osscube.spark.aerospike.rdd OPTIONS (initialHost \"192.168.142.162:3000\", select \"select * from test.one_million\", useUdfWithoutIndexQuery \"true\")")
```

NOTE: there is no explicit methods to write RDD back to Aerospike, but this can be achieved using code like this:

```
import com.aerospike.client.async.AsyncClient
import com.aerospike.client._

val rdd = sc.parallelize(List("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3)
rdd.foreachPartition{x =>
	val client = new AsyncClient("192.168.142.162" , 3000)
	x.foreach{ s =>
		client.put( client.asyncWritePolicyDefault, new Key("test", "sample", s),
			new Bin("column1", s)
		)
	}
}
```