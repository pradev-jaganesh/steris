package iotloganalytics


import org.apache.spark.SparkConf
import  org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
//import org.json4s.native.JsonFormats.parse
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import com.datastax.spark.connector._
//import com.datastax.bdp.spark.writer.BulkTableWriter._
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Row
import org.apache.spark.sql
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{SQLContext, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName


object KafkaCons_RE {  

 def main(args:Array[String])
  {
    
      val sparkConf = new SparkConf()
                         .setAppName("SterisIotAnalytics")
                         .setMaster("local[*]")
                         .set("spark.sql.crossJoin.enabled", "true")    
        
      val sparkcontext   = new SparkContext(sparkConf)
      val sqlContext     = new SQLContext(sparkcontext)
      val ssc            = new StreamingContext(sparkcontext, Seconds(10))
        
      //import sqlContext.implicits._
      //sparkcontext.setLogLevel("ERROR")
      
      val conf: Configuration = HBaseConfiguration.create()
        
        
        ssc.checkpoint("file:///tmp/checkpointdir")
        
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "@172.25.140.125:6667",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "STERIS",
          "auto.offset.reset" -> "latest"
          )

        val topics = Array("STERIS-DF-ORI")

        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )        
        
        val kafkastream = stream.map(record => (record.key, record.value))
        //kafkastream.print()
        val inputStream = kafkastream.map(rec => rec._2);
        //inputStream.print
      
        println("Before kafka3")
        
        inputStream.foreachRDD(rdd=>
            {   
              println("Before kafka4")
                //val jsonrdd = rdd.filter(_.contains("results"))
              
              conf.setInt("timeout", 120000)
                  println("After timeout")
              conf.set("hbase.zookeeper.quorum", "cch1wpsteris01:2181")
              println("After hbase.zookeeper.quorum")
              conf.set("zookeeper.znode.parent", "/hbase-unsecure") // IMPORTANT!!!
              println("After zookeeper.znode.parent")
              conf.setInt("hbase.client.scanner.caching", 10000)
              println("After hbase.client.scanner.caching")
              
              val connection: Connection = ConnectionFactory.createConnection(conf)
              println("After ConnectionFactory")
              val table = connection.getTable(TableName.valueOf("Iot_LogAnalytics_Rule_Engine"))
              print("connection created")
              
              val admin = connection.getAdmin
              // List the tables.
              val listtables = admin.listTables()
              
              listtables.foreach(println)
              connection.close()
        
//        if(!rdd.isEmpty)
//            {
//              rdd.foreach(println)
//              val df = sqlContext.read.text("rdd")
//              //df.show
//              println("Inside kafka")
//              val logwords = rdd.flatMap(_.split(" "))
//              println("-------------------------" +logwords)
//              
//              val logwordcounts = logwords.map(x => (x, 1)).reduceByKey(_ + _)
//              
//              }
    })  
    println("After kafka")
    ssc.start()
    ssc.awaitTermination()
  }    
 }