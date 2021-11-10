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

object KafkaCons {  

 def main(args:Array[String])
  {
    
   val sparkConf = new SparkConf()
                         .setAppName("SterisIotAnalytics")
    //                     .setMaster("local[*]")
                         .set("spark.sql.crossJoin.enabled", "true")    
        
    val sparkcontext = new SparkContext(sparkConf)
    val sqlContext   = new SQLContext(sparkcontext)
        
    import sqlContext.implicits._
    sparkcontext.setLogLevel("ERROR")
        
        val ssc = new StreamingContext(sparkcontext, Seconds(5))
        ssc.checkpoint("file:///tmp/checkpointdir")
        
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "@172.25.140.125:6667",
           //"bootstrap.servers" -> "35.209.4.97:9092",
          //"bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "STERIS",
          "auto.offset.reset" -> "earliest"
          )

        val topics = Array("STERIS-DF-ORI")

        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
        
        println("Before kafka")
        
        val kafkastream = stream.map(record => (record.key, record.value))
        println("Before kafka1")
        kafkastream.print()
        println("Before kafka2")
        val inputStream = kafkastream.map(rec => rec._2);
        inputStream.print
        println("Before kafka3")
        
        inputStream.foreachRDD(rdd=>
            {   
              println("Before kafka4")
                //val jsonrdd = rdd.filter(_.contains("results"))
        
        if(!rdd.isEmpty)
            {
              rdd.foreach(println)
              val df = sqlContext.read.text("rdd")
              //df.show
              println("Inside kafka")
              //val df1 = df.select(explode($"results"))
              //df1.write.text("hdfs:/user/hduser/ramdom1data.txt")
              //df1.saveAsTextFile()
              //df1.printSchema()
              }
    })  
    println("After kafka")
    ssc.start()
    ssc.awaitTermination()
  }    
 }