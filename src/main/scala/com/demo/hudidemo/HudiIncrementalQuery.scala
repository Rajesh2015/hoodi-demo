package com.demo.hudidemo

import com.typesafe.config.ConfigFactory
import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object HudiIncrementalQuery {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val hudiconfig = rootConfig.getConfig("hudi_conf")
    val sparkSession=SparkSession.builder().
      master("local").
      appName("Hudi query_example")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet","false")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")

    val hudiTablePath = s"s3a://${s3Config.getString("s3_bucket")}/${hudiconfig.getString("hudi_table_path")}"
    val hudiIncQueryDF = sparkSession
      .read
      .format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200711203922")
        //.option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY(), "/year=2020/month=*/day=*") // Optional, use glob pattern if querying certain partitions
      .load(hudiTablePath)


    hudiIncQueryDF.createOrReplaceTempView("hudi_table_incrimental")

    sparkSession.sql("select * from  hudi_table_incrimental").show()

  }

}
