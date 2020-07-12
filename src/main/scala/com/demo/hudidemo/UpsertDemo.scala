package com.demo.hudidemo

import com.typesafe.config.ConfigFactory
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object UpsertDemo {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val huddiConfig = rootConfig.getConfig("hudi_conf")

    val hudiTableName="hoodisampletable"

    val hudiOptions = Map[String,String](
      HoodieWriteConfig.TABLE_NAME → "huditable",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "Date",
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY ->"AccountNumber",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "Date"
    )


    val sparkSession=SparkSession.builder().
      master("local").
      appName("Hudi_example")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet","false")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")
    val conf = sparkSession.sparkContext.getConf
    val parquetFilePath = s"s3a://${s3Config.getString("s3_bucket")}/finances-small"

    val parquetDf=sparkSession.read.parquet(parquetFilePath).repartition(2)
    val filteredDf=parquetDf.filter(col("Date").between("3/1/2015","3/30/2015"))

    filteredDf.show(20,false)
    val hudiTablePath = s"s3a://${s3Config.getString("s3_bucket")}/${huddiConfig.getString("hudi_table_path")}"


    filteredDf.write
      .format("org.apache.hudi")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .options(hudiOptions)
      .mode(SaveMode.Append)
      .save(hudiTablePath)

    sparkSession.close()


  }

}
