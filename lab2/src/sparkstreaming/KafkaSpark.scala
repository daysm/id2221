package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and makes a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // throws errors from staxdrivers
    // create table manually
    //session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION { ’class’ : ’SimpleStrategy’, ’replication_factor’ : 1 };")
    //session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
      
    // spark context, streaming context
    val conf = new SparkConf().setAppName("Spark Streaming Example").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("~/work/lab2/src/sparkStreaming")

    
    // make a connection to Kafka and create Stream from message queue
    val kafkaConf = Map( "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")
      
    val topics = Set("avg")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)  
       

    // each message of stream has type (String, String) e.g. ("null", "a,2") 
    // map it to (String, Double): ("a", 2.0)
    val pairs = stream.map(record => (record._2.split(",")(0),record._2.split(",")(1).toDouble))
    // pairs.print(10) 

    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): Option[(String, (Double, Int))] = {
        
        if (state.exists() && !state.isTimingOut()) {
            val existingState = state.get()
            val existingAvg = existingState._1
            val existingCount = existingState._2
            val newAvg = (existingAvg * existingCount + value.getOrElse(existingAvg)) / (existingCount + 1)
            val newCount = existingCount + 1
            state.update((newAvg, newCount))
            Some(key, (newAvg, newCount))
        } else if (value.isDefined) {
            val initialValue = value.get.toDouble
            state.update((initialValue,1))
            Some(key, (initialValue, 1))
        } else {
            None
        }
    }
      
    // calculate average based on state
    // the state consists of (avg, count) 
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // stateDstream elements have form Option[(String, (Double, Int))] --> (key, (avg, count))
    // only write (key, avg) to database, because state has class option get is needed
    // (https://www.scala-lang.org/api/current/scala/Option.html)
    stateDstream.map(record => (record.get._1, record.get._2._1)).print()
    stateDstream.map(record => (record.get._1, record.get._2._1)).saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))   

    ssc.start()
    ssc.awaitTermination()
  }
}
