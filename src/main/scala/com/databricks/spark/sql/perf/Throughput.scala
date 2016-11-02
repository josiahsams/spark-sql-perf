/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import java.net.InetAddress

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.sql.perf.tpcds.TPCDS
import java.util.concurrent.TimeoutException

/**
 * Runs a query in a specfied duration and prints the finished iterations and the results to the screen.
 */
class Throughput() extends Serializable with Logging {

  def run(queryName: String, runDuration: Int) {

    val maxIterations = 10000;
    val tpcds = new TPCDS ()

    val conf = new SparkConf()

    val sc = SparkContext.getOrCreate(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)

    val allQueries= tpcds.tpcds1_4Queries.filter(_.name contains s"${queryName}-v1.4")

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = tpcds.runExperiment(
      executionsToRun = allQueries,
      iterations = maxIterations,
      tags = Map(
        "runtype" -> "cluster",
        "host" -> InetAddress.getLocalHost().getHostName()))

    println("== STARTING EXPERIMENT ==")
    try {
	    experiment.waitForFinish(runDuration)
    } catch {
	case e: TimeoutException => println(s"Got Timed out after ($runDuration seconds) !!")
	case _: Throwable => println("Got Killed with other exception ")
    }
    println("== END EXPERIMENT ==")

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
	  count("*") as 'count,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")
  }
}
