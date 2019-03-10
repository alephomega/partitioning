package com.kakaopage.crm.messaging

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DataSink, DataSource, DynamicFrame, GlueContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._


object  Partitioning {

  def source(glueContext: GlueContext, database: String, table: String, predicate: String): DataSource = {
    glueContext.getCatalogSource(
      database = database,
      tableName = table,
      pushDownPredicate = predicate)
  }

  def sink(glueContext: GlueContext, path: String, format: String): DataSink = {
    glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(f"""{"path": "s3://$path%s"}"""),
      format = format)
  }

  case class PartitioningParameters(database: String, table: String, predicate: String, path: String, format: String, partitions: Int, keys: Array[String])

  object PartitioningParameters {

    def apply(args: Map[String, String]): PartitioningParameters = {
      PartitioningParameters(
        get(args, "source-database"),
        get(args, "source-table"),
        get(args, "source-predicate", ""),
        get(args, "sink-path"),
        get(args, "sink-format"),
        get(args, "partitions").toInt,
        get(args, "partition-keys").split(":")
      )
    }

    private def get(args: Map[String, String], name: String, default: String = null): String = {
      args.get(name) match {
        case Some(v) => v
        case _ => {
          if (default != null)
            default
          else
            throw new RuntimeException("Required argument missing: " + name)
        }
      }
    }
  }

  def main(sysArgs: Array[String]) {
    val glueContext: GlueContext = new GlueContext(new SparkContext())

    val args = GlueArgParser.getResolvedOptions(
      sysArgs,
      Seq("JOB_NAME", "source-database", "source-table", "source-predicate", "sink-path", "sink-format", "partitions", "partition-keys").toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val params = PartitioningParameters(args)
    val df = source(glueContext, params.database, params.table, params.predicate).getDynamicFrame().toDF()
      .repartition(params.partitions, params.keys.map(k => col(k)): _*)

    sink(glueContext, params.path, params.format)
      .writeDynamicFrame(DynamicFrame(df, glueContext))

    Job.commit()
  }
}