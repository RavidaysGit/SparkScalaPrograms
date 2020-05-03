package com.spark.scala.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/* Input Test data(input.csv)
 * =========================
 * ID,Course,Subject,Marks
   101,MCA,Computer,70
	101,MCA,Account,80
	101,MCA,Datastructure,90
	102,MBA,SAP,30
	102,MBA,Account,50
	102,MBA,Marketing,60
	103,IT,Computer,20
	103,IT,Account,90
	103,IT,Datastructure,100
	104,EC,Computer,50
	104,EC,Electronics,45
	104,EC,Datastructure,55
 * 
 */

object PivotExample {

  def main(args: Array[String]): Unit = {
    //  val spark = SparkSession.builder.appName("MyFirtSprakProgram").master("local").getOrCreate()
    val spark = SparkSession.builder.appName("pivotExample").master("local").getOrCreate();
    import spark.implicits._
    var df = spark.read.option("header", true).option("inferschema", true).csv("C:/Users/ravi.kumar.narahari/Desktop/input.csv")
    //  df.printSchema()
    //var sel = df.select("ID","Course","Subject","Marks")
    //var df_pivot= df.groupBy("ID","Course").agg(sum("Marks"))

    var df_window_rank = df.withColumn("rank", rank().over(Window.partitionBy("ID", "Course", "Subject").orderBy("Marks")))
    //df_window_rank.show()

    var df_window_tot = df.withColumn("total", sum($"Marks").over(Window.partitionBy("ID", "Course")))
    //df_window_tot.show()

    var df_window_sum_piv = df_window_tot.groupBy("ID", "Course","total").pivot("Subject").sum("Marks").orderBy("ID")

    df_window_sum_piv.printSchema() 
    df_window_sum_piv.select($"ID",$"Course",$"total",when(col("Account").isNotNull,$"Account").otherwise(0).alias("Account"),
    when(col("Computer").isNotNull,$"Computer").otherwise(0).alias("Computer"),
    when(col("Datastructure").isNotNull,$"Datastructure").otherwise(0).alias("Datastructure"),
    when(col("Electronics").isNotNull,$"Electronics").otherwise(0).alias("Electronics"),
    when(col("Marketing").isNotNull,$"Marketing").otherwise(0).alias("Marketing"),
     when(col("SAP").isNotNull,$"SAP").otherwise(0).alias("SAP")
    ).show()
  }
}