package com.demo.hudidemo

import com.typesafe.config.ConfigFactory
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.SparkSession

object HudiSnapShotQueryDemo {
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
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(hudiTablePath + "/*/*") //The number of wildcard asterisks here must be one greater than the number of partition


    hudiIncQueryDF.createOrReplaceTempView("hudi_table")

    sparkSession.sql("select * from  hudi_table").show()

    /**
     * checking after delete this records should not  be there
     */
    sparkSession.sql("select * from  hudi_table where Date between '3/1/2015' and '3/30/2015'").show()

  }

}
