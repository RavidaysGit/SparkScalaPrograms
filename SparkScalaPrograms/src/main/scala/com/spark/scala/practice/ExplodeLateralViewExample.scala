package com.spark.scala.practice

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.Seq

/*  input test data(explode_input.csv)
 * ==================================
 * 	ID|Phone|City
		1|1234567890,9876543210|BLR,ND
		2|2345678910,8765432190|CH,MB
 * 
 *  
 */

object ExplodeLateralViewExample {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder.appName("pivotExample").master("local").getOrCreate();
    import spark.implicits._
    var df = spark.read.option("header", true).option("inferschema", true).option("quote",",")
            .option("delimiter","|").csv("C:/Users/ravi.kumar.narahari/Desktop/explode_input.csv")
     var dff= df.withColumn("phone_no",split($"Phone",",")).withColumn("city_name",split($"City",",")).drop("Phone").drop("City")
           //dff.printSchema()
           //dff.show()
   var df_ex = dff.select($"ID",posexplode($"phone_no") as Seq("pos1", "phone"),$"city_name")
  // df_ex.show()
   var df_exx = df_ex.select($"ID",$"pos1",$"phone",posexplode($"city_name") as Seq("pos2","city"))
   df_exx.show()
   df_exx.select("ID","phone","city").where("pos1==pos2").show()
   
  }
}