package com.demo.hudidemo

import com.typesafe.config.ConfigFactory
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object WriteDemoWithMergeOnRead {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val huddiConfig = rootConfig.getConfig("hudi_conf")

    val hudiTableName="hoodisampletable"

    val hudiOptions = Map[String,String](
      HoodieWriteConfig.TABLE_NAME → "huditable",
      DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> "MERGE_ON_READ",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "Date",//recordkey
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY ->"AccountNumber",//default->partitionpath
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
    val parquetFilePath = s"s3a://${s3Config.getString("s3_bucket")}/finances-small"

    val parquetDf=sparkSession.read.parquet(parquetFilePath).repartition(2)
    val filteredDf=parquetDf.filter(col("Date").between("1/1/2015","2/30/2015"))//Taking Only two months to Hudi table
    filteredDf.show(20,false)
    val hudiTablePath = s"s3a://${s3Config.getString("s3_bucket")}/${huddiConfig.getString("hudi_mor_table_path")}"

    filteredDf.write
      .format("org.apache.hudi")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(hudiTablePath)

    sparkSession.close()
  }
}
