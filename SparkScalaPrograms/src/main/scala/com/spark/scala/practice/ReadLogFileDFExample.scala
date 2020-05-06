package com.spark.scala.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/* // Input data file
 192.168.198.92 - - "[22/Dec/2002:23:08:37 -0400]" "GET 
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
 * 
 * 
 * // out put 
 * 
 * +--------------+---+---+----------------------------+---------------------------------+---+-----+-------------+--------------------------------+-------------------------------------------------------+----+
|_c0           |_c1|_c2|_c3                         |_c4                              |_c5|_c6  |_c7          |_c8                             |_c9                                                    |_c10|
+--------------+---+---+----------------------------+---------------------------------+---+-----+-------------+--------------------------------+-------------------------------------------------------+----+
|192.168.198.92|-  |-  |[22/Dec/2002:23:08:37 -0400]|GET    / HTTP/1.1                |200|6394 |www.yahoo.com|-                               |Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1...)  |-   |
|192.168.198.92|-  |-  |[22/Dec/2002:23:08:38 -0400]|GET    /images/logo.gif HTTP/1.1 |200|807  |www.yahoo.com|http://www.some.com/            |Mozilla/4.0 (compatible; MSIE 6...)                    |-   |
|192.168.72.177|-  |-  |[22/Dec/2002:23:32:14 -0400]|GET    /news/sports.html HTTP/1.1|200|3500 |www.yahoo.com|http://www.some.com/            |Mozilla/4.0 (compatible; MSIE ...)                     |-   |
|192.168.72.177|-  |-  |[22/Dec/2002:23:32:14 -0400]|GET    /favicon.ico HTTP/1.1     |404|1997 |www.yahoo.com|-                               |Mozilla/5.0 (Windows; U; Windows NT 5.1; rv:1.7.3)...  |-   |
|192.168.72.177|-  |-  |[22/Dec/2002:23:32:15 -0400]|GET    /style.css HTTP/1.1       |200|4138 |www.yahoo.com|http://www.yahoo.com/index.html |Mozilla/5.0 (Windows...                                |-   |
|192.168.72.177|-  |-  |[22/Dec/2002:23:32:16 -0400]|GET    /js/ads.js HTTP/1.1       |200|10229|www.yahoo.com|http://www.search.com/index.html|Mozilla/5.0 (Windows...                                |-   |
|192.168.72.177|-  |-  |[22/Dec/2002:23:32:19 -0400]|GET    /search.php HTTP/1.1      |400|1997 |www.yahoo.com|-                               |Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ...)|-   |
+--------------+---+---+----------------------------+---------------------------------+---+-----+-------------+--------------------------------+-------------------------------------------------------+----+
 * 
 */
object ReadLogFileDFExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("RegexExample").master("local").getOrCreate();
    import spark.implicits._
  /*  var df = spark.read.option("inferSchema", true)
      .option("inferSchema", true).option("multiLine", true).option("escape", "\"").option("escape", "\n")
      .option("delimiter", ",")
      .csv("C:/Users/ravi.kumar.narahari/Desktop/log_input.csv")*/
      
    var df =  spark.read.option("sep", " ").option("quote", "\"").option("multiLine", "true").option("escape", "\"").option("inferSchema", "true")
    .option("escape", "\n")
      .csv("C:/Users/ravi.kumar.narahari/Desktop/log_input.csv")

    df.show(false)

  }

}