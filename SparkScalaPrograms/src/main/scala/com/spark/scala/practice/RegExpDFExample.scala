package com.spark.scala.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/*
 * //Input data file
 * 
 * 192.168.198.92 - - "[22/Dec/2002:23:08:37 -0400]" "GET 
   / HTTP/1.1" 200 6394 www.yahoo.com "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1...)" "-"
192.168.198.92 - - "[22/Dec/2002:23:08:38 -0400]" "GET 
   /images/logo.gif HTTP/1.1" 200 807 www.yahoo.com "http://www.some.com/" "Mozilla/4.0 (compatible; MSIE 6...)" "-"
192.168.72.177 - - "[22/Dec/2002:23:32:14 -0400]" "GET 
   /news/sports.html HTTP/1.1" 200 3500 www.yahoo.com "http://www.some.com/" "Mozilla/4.0 (compatible; MSIE ...)" "-"
192.168.72.177 - - "[22/Dec/2002:23:32:14 -0400]" "GET 
   /favicon.ico HTTP/1.1" 404 1997 www.yahoo.com "-" "Mozilla/5.0 (Windows; U; Windows NT 5.1; rv:1.7.3)..." "-"
192.168.72.177 - - "[22/Dec/2002:23:32:15 -0400]" "GET 
   /style.css HTTP/1.1" 200 4138 www.yahoo.com "http://www.yahoo.com/index.html" "Mozilla/5.0 (Windows..." "-"
192.168.72.177 - - "[22/Dec/2002:23:32:16 -0400]" "GET 
   /js/ads.js HTTP/1.1" 200 10229 www.yahoo.com "http://www.search.com/index.html" "Mozilla/5.0 (Windows..." "-"
192.168.72.177 - - "[22/Dec/2002:23:32:19 -0400]" "GET 
   /search.php HTTP/1.1" 400 1997 www.yahoo.com "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ...)" "-"
 * 
 * //OUT put 
 * 
 * +--------------+--------------------+------+-------------+------------+
|IP_ADDRESS    |DATE                |METHOD|LINK         |BROWSER_NAME|
+--------------+--------------------+------+-------------+------------+
|192.168.198.92|22/Dec/2002:23:08:37|GET   |www.yahoo.com|Mozilla/4.0 |
|192.168.198.92|22/Dec/2002:23:08:38|GET   |www.yahoo.com|Mozilla/4.0 |
|192.168.72.177|22/Dec/2002:23:32:14|GET   |www.yahoo.com|Mozilla/4.0 |
|192.168.72.177|22/Dec/2002:23:32:14|GET   |www.yahoo.com|Mozilla/5.0 |
|192.168.72.177|22/Dec/2002:23:32:15|GET   |www.yahoo.com|Mozilla/5.0 |
|192.168.72.177|22/Dec/2002:23:32:16|GET   |www.yahoo.com|Mozilla/5.0 |
|192.168.72.177|22/Dec/2002:23:32:19|GET   |www.yahoo.com|Mozilla/4.0 |
+--------------+--------------------+------+-------------+------------+
 * 
 * 
 */
object RegExpDFExample {
  
  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder.appName("RegexExample").master("local").getOrCreate();
    import spark.implicits._
    var df =  spark.read.option("sep", " ")
    .option("quote", "\"")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("inferSchema", "true")
    .option("escape", "\n")
     .csv("C:/Users/ravi.kumar.narahari/Desktop/log_input.csv")
     
    var dff= df.select(col("_c0").alias("ip_address"),col("_c3").alias("date"),col("_c4").alias("ws_metod"),col("_c7")
         .alias("url"),col("_c9").alias("broswer_name"))
         
        var dr= dff.withColumn("date_change",regexp_extract(col("date"),"(\\d{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2})", 0))
                   .withColumn("ws_method_change",regexp_extract(col("ws_metod"), "([A-Z]*)", 0))
                   .withColumn("broswer_name_change",regexp_extract(col("broswer_name"), "([A-za-z]*/\\d+.\\d+)",0))
                   .drop(col("date")).drop(col("ws_metod")).drop(col("broswer_name"))
var drr= dr.select(col("ip_address").alias("IP_ADDRESS"),col("date_change").alias("DATE"),col("ws_method_change").alias("METHOD"),col("url").alias("LINK"),col("broswer_name_change").alias("BROWSER_NAME"))
    drr.show(false)
  }
}