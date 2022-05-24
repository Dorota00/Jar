package agh

import java.time.Year
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Movies extends SparkSessionProvider {

  def main(args: Array[String]): DataFrame = {
    val path = args(0)

    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .na.drop()
      .withColumn("years_passed", (col("year").cast("int") - Year.now.getValue) * -1)

  }
}