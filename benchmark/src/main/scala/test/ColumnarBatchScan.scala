package test

import org.apache.spark.sql.SparkSession

object ColumnarBatchScan {
  def main(args:Array[String]) {
    val fieldsNum = if (args.length > 0) args(0).toInt else 1
    val spark = SparkSession.builder.appName(s"ColumnarBatchScan_$fieldsNum").config("org.apache.spark.example.columnar.enabled", "true").config("spark.sql.extensions", "org.apache.spark.columnarPlugin.ColumnarPlugin").getOrCreate()
    var df = spark.read.format("parquet").load("/tpcds/web_sales")
   
    fieldsNum match {
      case 1 => 
        df = df.select("ws_sold_time_sk")
      case 2 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk")
      case 3 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk")
      case 4 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk")
      case 5 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk")
      case 6 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk")
      case 7 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk")
      case 8 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk")
      case 9 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk")
      case 10 => 
        df = df.select("ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk")
      case _ => 
        throw new Exception("arg not support")
    }
    
    //df.write.format("parquet").save("/tpcds_output/web_sales/")
    
    df.foreach(r => {
    })
    spark.stop()
  }
}
