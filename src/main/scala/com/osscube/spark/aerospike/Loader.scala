package com.osscube.spark.aerospike

import java.security.SecureRandom

import com.aerospike.client.AerospikeClient
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.query.Filter

object Loader {
  val random = new SecureRandom()
  def randomString(characterSet : String,  length: Int) :String = {
    val s = for  {i <-  (0 until length)
        randomCharIndex = random.nextInt(characterSet.length)
        result = characterSet(randomCharIndex)
    } yield result

    return s.seq.mkString("")
  }


  def main(args: Array[String]) {


    val policy = new ClientPolicy()
    policy.failIfNotConnected = true
    val client = new AerospikeClient(policy, "192.168.142.162" , 3000)
    val begin : Long = System.currentTimeMillis()
    val CHARSET_AZ_09 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    for(i <- 1 until 1000000)
    {
      client.put(policy.writePolicyDefault, new Key("test", "one_million", randomString(CHARSET_AZ_09, 5)),
        new Bin("column1", randomString(CHARSET_AZ_09, 1)),
        new Bin("column2", randomString(CHARSET_AZ_09, 2)),
        new Bin("column3", randomString(CHARSET_AZ_09, 3)),
        new Bin("column10", randomString(CHARSET_AZ_09, 10)),
        new Bin("column50", randomString(CHARSET_AZ_09, 50)),
        new Bin("column100", randomString(CHARSET_AZ_09, 100)),
        new Bin("column300", randomString(CHARSET_AZ_09, 300)),
        new Bin("longColumn1", random.nextLong),
        new Bin("intColumn1", random.nextInt)
      )

      //Thread.sleep(1);
    }

    val end = System.currentTimeMillis()
    val seconds =  (end - begin) / 1000.0
    System.out.println(seconds)
    client.close()



    //System.out.println (parseSelect("select bin1 from namespace where bin2 between 0 and 2", 3))

  }

  def removeDoubleSpaces (s:String): String = if(!s.contains("  ")) s else removeDoubleSpaces(s.replace("  "," "))

  //Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

  def parseSelect(s: String, numPartitionsPerServerForRange: Int): (String, String, Seq[String], Int, String, Seq[(Any, Long)]) = {

    if (s != null && !s.isEmpty) {
      val tokenised = removeDoubleSpaces(s.replace("=", " = ")).replace(", ", ",").replace(" ,", ",").split(" ")
      if (tokenised(0).toUpperCase != "SELECT")
        throw new Exception("Cant parse the statement, missing select: " + s)

      val bins = tokenised(1).split(",")

      if (tokenised(2).toUpperCase != "FROM")
        throw new Exception("Cant parse the statement, missing from: " + s)


      val namespaceAndSet = tokenised(3).split("\\.")
      val namespace = namespaceAndSet(0)
      val set = if (namespaceAndSet.length > 1) namespaceAndSet(1) else ""

      if (tokenised.length > 7 && tokenised(4).toUpperCase == "WHERE") {
        val positionOfBetween: Int = tokenised.map(_.toUpperCase).indexOf("BETWEEN")
        val positionOfAnd: Int = tokenised.map(_.toUpperCase).indexOf("AND")
        val positionOfEq: Int = tokenised.indexOf("=")
        val trimmedBin: String = tokenised(5).trim

        if (positionOfBetween == 6 && positionOfAnd == 8) {
          //Process range query here
          val lower: Long = tokenised(7).toLong
          val upper: Long = tokenised(9).toLong
          var tuples: Seq[(Long, Long)] = Seq((lower, upper))
          val range: Long = upper - lower
          if(numPartitionsPerServerForRange > 1 && range >= numPartitionsPerServerForRange) {

            val divided = range / numPartitionsPerServerForRange
            tuples =  (0 until numPartitionsPerServerForRange).map(i => (lower + divided*i , if(i == numPartitionsPerServerForRange -1) upper else lower + divided*(i +1) -1))

          }
          return (namespace, set, bins, 3, trimmedBin, tuples)
        }
        else if (positionOfEq == 6) {
          //Process equals query here
          if (tokenised(7).forall(n => n.isDigit || n == '-'))
            return (namespace, set, bins, 2, trimmedBin, Seq((tokenised(7).toLong, 0L)))
          else
            return (namespace, set, bins, 1, trimmedBin, Seq((tokenised(7), 0L)))
        }
        else return (namespace, set, bins, 0, "", Seq())
      }
      else return (namespace, set, bins, 0, "", Seq())

    }
    null
  }
}
