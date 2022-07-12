package com.test

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object test extends App {

  val spark = SparkSession.builder()
    .appName("testing spark")
    .config("spark.master","local")
    .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
    .getOrCreate()


  // historycal DB
  val driver_historical = "org.postgresql.Driver"
  val url_historical = "jdbc:postgresql://localhost:5432/HistorycalDB"
  val user_historical = "postgres"
  val password_historical = 221299

  def readTable_Historical(tableName:String) = spark.read
    .format("jdbc")
    .option("driver",driver_historical)
    .option("url",url_historical)
    .option("user",user_historical)
    .option("password",password_historical)
    .option("dbtable",s"public.$tableName")
    .load()

  val distribution_centers = readTable_Historical("dim_distribution_centers")
  val ordersDF = readTable_Historical("dim_orders")
  val products = readTable_Historical("dim_products")
  val employees = readTable_Historical("fact_employees")
  val events = readTable_Historical("fact_events")
  val inventory_items = readTable_Historical("fact_inventory_items")
  val order_items= readTable_Historical("fact_order_items")
  val users = readTable_Historical("fact_users")

  /**
  products.createOrReplaceTempView("products")
  val products_test = spark.sql(
    """
      |select category from products where category = 'Accessories'
      |""".stripMargin
  )

  **/
  def convertTable(tableName:List[String]) = tableName.foreach{tableName=>
    val tableDF = readTable_Historical(tableName)
    tableDF.createOrReplaceTempView(tableName)
  }

  convertTable(List(
    "dim_distribution_centers",
    "dim_orders",
    "dim_products",
    "fact_employees",
    "fact_events",
    "fact_inventory_items",
    "fact_order_items",
    "fact_users"
  ))

  val dim_products_join = spark.sql(
    """
      |select A.id as product_id,
      |       A.cost,
      |       A.category,
      |       A.name,
      |       A.brand,
      |       A.department,
      |       B.name as distribution_center
      |from dim_products as A
      |inner join dim_distribution_centers as B
      |on A.distribution_center_id = B.id
      |""".stripMargin
  )
  //dim_products_join.show()

  val dim_orders_join = spark.sql(
    """
      |select order_id,user_id,status,gender,created_at,shipped_at,num_of_item
      |from dim_orders
      |""".stripMargin
  )
  //dim_orders_join.show()

  val dim_users_join = spark.sql(
    """
      |select id as user_id,
      |       first_name,
      |       last_name,
      |       age,
      |       gender,
      |       state,
      |       street_address,
      |       city,
      |       country,
      |       traffic_source,
      |       created_at
      | from fact_users
      |""".stripMargin
  )
  //dim_users.show()

  val fact_orders_item_join = spark.sql(
    """
      |select id,
      |       order_id,
      |       user_id,
      |       product_id,
      |       inventory_item_id,
      |       status,
      |       sale_price,
      |       created_at,
      |       shipped_at,
      |       delivered_at
      |from fact_order_items
      |""".stripMargin
  )
  //fact_orders_item.show()

  val dim_inventory_items_join = spark.sql(
    """
      |select A.id as inventory_item_id,
      |       A.product_id,
      |       A.product_name,
      |       A.product_category,
      |       A.product_brand,
      |       A.product_department,
      |       A.cost,
      |       A.product_retail_price,
      |       B.name as distribution_center,
      |       A.created_at,
      |       A.sold_at
      |from fact_inventory_items A
      |inner join dim_distribution_centers B
      |on A.product_distribution_center_id = B.id
      |""".stripMargin
  )
  //dim_inventory_items_join.show()

  // send data to postgre dwh
    // postgre DWH configure
  val driver2 = "org.postgresql.Driver"
  val url2 = "jdbc:postgresql://localhost:5432/finalProjectDWH"
  val user2 = "postgres"
  val password2 = 221299

  dim_products_join.write
    .format("jdbc")
    .option("driver",driver2)
    .option("url",url2)
    .option("user",user2)
    .option("password", password2)
    .option("dbtable", "public.dim_products")
    .mode("ignore")
    .save()

  dim_orders_join.write
    .format("jdbc")
    .option("dirver",driver2)
    .option("url",url2)
    .option("user",user2)
    .option("password",password2)
    .option("dbtable","public.dim_orders")
    .mode("ignore")
    .save()

  dim_users_join.write
    .format("jdbc")
    .option("driver",driver2)
    .option("url",url2)
    .option("user",user2)
    .option("password",password2)
    .option("dbtable","public.dim_users")
    .mode("ignore")
    .save()

  fact_orders_item_join.write
    .format("jdbc")
    .option("driver", driver2)
    .option("url",url2)
    .option("user",user2)
    .option("password",password2)
    .option("dbtable","public.fact_orders_item")
    .mode("ignore")
    .save()

  dim_inventory_items_join.write
    .format("jdbc")
    .option("driver",driver2)
    .option("url",url2)
    .option("user",user2)
    .option("password",password2)
    .option("dbtable", "public.dim_inventory_items")
    .mode("ignore")
    .save()



}
