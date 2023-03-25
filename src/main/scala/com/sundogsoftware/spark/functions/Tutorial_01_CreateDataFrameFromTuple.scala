package com.sundogsoftware.spark.dataframe

import com.sundogsoftware.spark.utils.Context

/**

  */
object Tutorial_01_CreateDataFrameFromTuple extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))

  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  df.show()
}
