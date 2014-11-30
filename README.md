Aerospike Spark Connector
===============

Spark glue to efficiently read data from Aerospike

  * Creates schema RDD from AQL statement (including determining datatypes on the single row query) or just RDD[Row] if not to be used with SparkSQL context
  * Queries local Aoerospike nodes in parallel (allows parrallel reads from single server for range queries)
  
Example use:

```
import com.osscube.spark.aerospike.rdd._;import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
val aero  = sc.aeroSInput(("192.168.142.162" , 3000), "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000", sqlContext)
aero.registerTempTable("aero")
sqlContext.sql("select avg(intColumn1) from aero where intColumn1 < 0").collect

```
(Assumes there is an numeric index on intColumn1)


