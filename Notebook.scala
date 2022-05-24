// Databricks notebook source
import agh._

// COMMAND ----------

val params: Array[String] = Array("/FileStore/tables/Files/movies.csv")

// COMMAND ----------

val df = Movies.main(params)

// COMMAND ----------

display(df)
