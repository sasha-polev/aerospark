package main.scala.com.osscube.spark.aerospike

import java.security.SecureRandom

import com.aerospike.client.AerospikeClient
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.Bin
import com.aerospike.client.Key

object Loader {
  val random = new SecureRandom()
  def randomString(characterSet : String,  length: Int) :String = {
    val s = for  {i <-  (0 until length-1)
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
      val randKey = randomString(CHARSET_AZ_09, 5)

      client.put(policy.writePolicyDefault, new Key("test", "one_million", randKey),
        new Bin("10chars", randomString(CHARSET_AZ_09, 10)),
        new Bin("50chars", randomString(CHARSET_AZ_09, 50)),
        new Bin("100chars", randomString(CHARSET_AZ_09, 100)),
        new Bin("300chars", randomString(CHARSET_AZ_09, 300)),
        new Bin("long", random.nextLong),
        new Bin("int", random.nextInt)
      )
      //Thread.sleep(1);
    }

    val end = System.currentTimeMillis()
    val seconds =  (end - begin) / 1000.0
    System.out.println(seconds)
    client.close()

  }



}
