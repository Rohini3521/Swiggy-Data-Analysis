package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, when}

object MainSwiggy {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // creating spark session
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Swiggy")
      .getOrCreate()

    // Read Json input data into dataframe
    val df = spark.read.option("multiline", "true")
      .format("json")
      .load("C:/SparkScala/SparkScala/data/ml-100k/swiggy.json")

    // select required columns
    val selectdf = df.select("cust_name", "id", "name", "city", "avgrating", "cuisines",
      "costfortwos", "deliverytime", "mindeliverytime", "maxdeliverytime", "locality", "date", "amount")

    // drop all the null values from dataframe
    val drop_null = selectdf.na.drop("all")

    // fill value "Unknown" in the column "cust_name" if it is null
    // fill value "N/A" in listed columns if they are null
    val replce_null = drop_null.na.fill("Unknown", Array("cust_name"))
      .na.fill("N/A", Array("id", "name", "city", "avgrating", "cuisines"
      , "costfortwos", "deliverytime", "mindeliverytime", "maxdeliverytime", "locality", "date", "amount"))

    // create year,month,day columns using date column
    val date_df = replce_null
      .withColumn("year", col("date")
        .substr(5, 4))
      .withColumn("month", col("date")
        .substr(3, 2))
      .withColumn("day", col("date")
        .substr(1, 2))


    // replace Road with Rd,street with St and Avenue with Ave in the column "locality"
    val ShortForm = date_df.withColumn("locality", regexp_replace(col("locality"), "Road", "Rd"))
      .withColumn("locality", regexp_replace(col("locality"), "Street", "St"))
      .withColumn("locality", regexp_replace(col("locality"), "Avenue", "Ave"))

    // Create a new column "delivery_rating",rating is 4 if delivery time is between 10 to 40 minutes, otherwise 3
    var ratingdf = ShortForm.withColumn(colName = "delivery_rating", when(col("deliverytime") >= "10"
      && col("deliverytime") <= "40", "4")
      .otherwise("3"))


  }

}
