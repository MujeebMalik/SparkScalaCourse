package com.sundogsoftware.spark.dataframe

import com.sundogsoftware.spark.utils.Context

/**

  */
object Tutorial_10_DataFrameColumnWithUDF extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")


  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._


  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut"    => Seq(100, 500)
    case "vanilla donut"  => Seq(200, 400)
    case "glazed donut"   => Seq(300, 600)
    case _                => Seq(150, 150)
  }

  val udfStockMinMax = udf(stockMinMax)
  val df2 = df.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
  df2.show()
}
