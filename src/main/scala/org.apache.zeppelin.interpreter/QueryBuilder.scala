package org.apache.zeppelin.interpreter

import scala.collection.mutable

import org.apache.zeppelin.spark.ZeppelinContext

import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}

import org.apache.spark.sql._
import org.apache.spark.SparkContext

object QueryBuilder {

  // File formats
  lazy val csv = "CSV"
  lazy val csv_delimiter = "delimiter"
  lazy val csv_charset = "charset"
  lazy val csv_quote = "quote"
  lazy val csv_escape = "escape"
  lazy val csv_comment = "comment"
  lazy val csv_header = "header"
  lazy val csv_inferSchema = "inferSchema"
  lazy val csv_mode = "mode"
  lazy val csv_ignoreLeadingWhiteSpace = "ignoreLeadingWhiteSpace"
  lazy val csv_ignoreTrailingWhiteSpace = "ignoreTrailingWhiteSpace"
  lazy val csv_nullValue = "nullValue"
  lazy val csv_nanValue = "nanValue"
  lazy val csv_positiveInf = "positiveInf"
  lazy val csv_negativeInf = "negativeInf"
  lazy val csv_compressionCodec = "compressionCodec"
  lazy val csv_dateFormat = "dateFormat"
  lazy val csv_timestampFormat = "timestampFormat"
  lazy val csv_maxColumns = "maxColumns"
  lazy val csv_maxCharsPerColumn = "maxCharsPerColumn"
  lazy val csv_escapeQuotes = "escapeQuotes"
  lazy val csv_maxMalformedLogPerPartition = "maxMalformedLogPerPartition"
  lazy val csv_quoteAll = "quoteAll"
  lazy val csv_options = List(csv_delimiter, csv_charset, csv_quote, csv_escape, csv_comment, csv_header,
    csv_inferSchema, csv_mode, csv_ignoreLeadingWhiteSpace, csv_ignoreTrailingWhiteSpace,
    csv_nullValue, csv_nanValue, csv_positiveInf, csv_negativeInf, csv_compressionCodec,
    csv_dateFormat, csv_timestampFormat, csv_maxColumns, csv_maxCharsPerColumn, csv_escapeQuotes,
    csv_maxMalformedLogPerPartition, csv_quoteAll
  )


  lazy val prq = "Parquet"
  lazy val json = "JSON"
  lazy val orc = "ORC"
  lazy val avro = "Avro"

  lazy val xml = "XML"
  lazy val xml_rowtag = "rowTag"

  lazy val txt = "Text"
  lazy val txt_header = "text_header"
  lazy val txt_delimiter = "text_delimiter"

  // Data sources and parameters
  lazy val hdfs = "hdfs"
  lazy val hdfs_namenode = "namenode"
  lazy val hdfs_path = "path"

  lazy val aws = "AWS"
  lazy val aws_id = "id"
  lazy val aws_secret = "secret"
  lazy val aws_location = "location"
  lazy val aws_accessor = "accessor"

  lazy val lfs = "LFS"
  lazy val lfs_path = "path"

  lazy val gcs = "GCS"
  lazy val gcs_projID = "projectid"
  lazy val gcs_keyPath = "keypath"
  lazy val gcs_bucket = "bucket"
  lazy val gcs_path = "path"
  lazy val gcs_accessor = "gs"

  lazy val adls = "ADLS"
  lazy val adls_storage = "storageAccount"
  lazy val adls_key = "key"
  lazy val adls_container = "container"
  lazy val adls_filepath = "filepath"
  lazy val adls_accessor = "wasb"
  lazy val adls_domain = ".blob.core.windows.net"

  private lazy val fileFormats = List(("default", "select file format"), (csv, csv), (prq, prq), (json, json), (orc, orc), (avro, avro), (xml, xml), (txt, txt))
  private lazy val booleanOpts = List(("true", "true"), ("false", "false"))
  private lazy val sources = List((hdfs, "Hadoop File System"), (aws, "Amazon S3"), (lfs, "Local File System"),
    (gcs, "Google Cloud Storage"), (adls, "Microsoft Azure Store"))

  private val dlSlash = "/"
  private val dlColon = ":"
  private val dlColonSlash = dlColon + dlSlash + dlSlash
  private val dlAtRate = "@"

  private val tableStyle =
    s"""
            <style>
            table, th, td {
                border: 1px solid black;
                }

                th, td {
                  padding: 2px;
                }
            </style>
           """

  private def renderButtons(bind: String, paraID: String, unbind: String, executeNextParagraph: Boolean) = {
     s"""<button type="submit" class="btn btn-primary" ng-click="${bind};
              ${if (executeNextParagraph) s"z.runParagraph('${paraID}')" else ""}"
              onClick="update('Accepted successfully.')">Confirm</button>
            <button type="reset" class="btn btn-primary" ng-click="${unbind};">Reset</button>
            <script>
              function update(message){
                alert(message)
              }
            </script>"""
  }

  case class ParamText(name: String,
      desc: String,
      default: String,
      opts: Option[List[(String, String)]] = None,
      secure: Option[String] = None)

  def renderParamList(pList: List[ParamText], paraID: String, executeNextParagraph: Boolean = false): String = {
    val s = new StringBuilder()
    val bind = new StringBuilder()
    val unbind = new StringBuilder()
    for (p <- pList) {
      (p.opts) match {
        case (Some(x)) => {
          s ++=
              s"""<mat-form-field><label for="select${p.name}" style="width:200px">${p.name}</label>
                    <select matNativeControl required ng-model="${p.name}">"""
          for (m <- x)
            s ++= s"""<option value="${m._1}" ${if ((m._2) == p.default) "selected" else ""}>${m._2}</option>"""
          s ++= s"""</select></mat-form-field><br/>"""
          bind ++= s"""z.angularBind('${p.name}',${p.name},'${paraID}');"""
          unbind ++= s"""z.angularUnbind('${p.name}','${paraID}');"""
        }
        case _ => {
          s ++=
              s"""<label style="width:200px" for="${p.name}">${p.desc}</label> <input type=${
                (p.secure) match {
                  case Some(y) => y
                  case _ => "text"
                }
              } class="form-control" id="${p.name}" ng-model="${p.name}" ng-init="${p.name}='${p.default}'"}
                </input><br/>"""
          bind ++= s"""z.angularBind('${p.name}',${p.name},'${paraID}');"""
          unbind ++= s"""z.angularUnbind('${p.name}','${paraID}');"""
        }
      }
    }

    s"""%angular
        <form class="form-inline">
        <div>
            ${s.toString}
            ${renderButtons(bind.toString, paraID, unbind.toString, executeNextParagraph)}
        </div>
        </form>
        """
  }


  def getPathFromParams(z: ZeppelinContext, ds: String) = ds match {
    case `hdfs` => hdfs + dlColonSlash + z.angular(hdfs_namenode).asInstanceOf[String] + dlSlash +
        z.angular(hdfs_path).asInstanceOf[String]
    case `aws` => z.angular(aws_accessor) + dlColonSlash + z.angular(aws_id).asInstanceOf[String] + dlColon +
        z.angular(aws_secret).asInstanceOf[String] + dlAtRate + z.angular(aws_location).asInstanceOf[String]
    case `gcs` => gcs_accessor + dlColonSlash + z.angular(gcs_bucket).asInstanceOf[String] +
        dlSlash + z.angular(gcs_path).asInstanceOf[String]
    case `adls` => adls_accessor + dlColonSlash + z.angular(adls_container).asInstanceOf[String] +
        dlAtRate + z.angular(adls_storage).asInstanceOf[String] +
        adls_domain + dlSlash + z.angular(adls_filepath).asInstanceOf[String]
    case `lfs` => z.angular(lfs_path).asInstanceOf[String]
    case _ => ""
  }

  //def configureDataSourceEnvParams(sc : org.apache.spark.SparkContext,params : scala.collection.mutable.Map[String,String], ds : String) = ds match {
  def configureDataSourceEnvParams(sc: org.apache.spark.SparkContext, z: ZeppelinContext, ds: String) = ds match {
    case `gcs` => {
      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      sc.hadoopConfiguration.set("fs.gs.project.id", z.angular(gcs_projID).asInstanceOf[String])
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", z.angular(gcs_keyPath).asInstanceOf[String])
    }
    case `adls` => {
      sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + z.angular(adls_storage).asInstanceOf[String] + adls_domain, z.angular(adls_key).asInstanceOf[String])
    }
    case `aws` => {
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsAccessKeyId", z.angular(aws_id).asInstanceOf[String])
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsSecretAccessKey", z.angular(aws_secret).asInstanceOf[String])
    }
    case _ =>
  }

  def getFileFormatParams(f: String): List[ParamText] = f match {
    case `csv` => List(
      ParamText(csv_delimiter, csv_delimiter, ","),
      ParamText(csv_charset, csv_charset, "UTF-8"),
      ParamText(csv_quote, csv_quote, "\""),
      ParamText(csv_escape, csv_escape, "\\"),
      ParamText(csv_comment, csv_comment, "\u0000"),
      ParamText(csv_header, csv_header, "false", Some(booleanOpts)),
      ParamText(csv_inferSchema, csv_inferSchema, "false", Some(booleanOpts)),
      ParamText(csv_mode, csv_mode, "DROPMALFORMED", Some(List(("DROPMALFORMED", "DROPMALFORMED"), ("FAILFAST", "FAILFAST"), ("PERMISSIVE", "PERMISSIVE")))),
      ParamText(csv_ignoreLeadingWhiteSpace, csv_ignoreLeadingWhiteSpace, "false", Some(booleanOpts)),
      ParamText(csv_ignoreTrailingWhiteSpace, csv_ignoreTrailingWhiteSpace, "false", Some(booleanOpts)),
      ParamText(csv_nullValue, csv_nullValue, ""),
      ParamText(csv_nanValue, csv_nanValue, "NaN"),
      ParamText(csv_positiveInf, csv_positiveInf, "Inf"),
      ParamText(csv_negativeInf, csv_negativeInf, "-Inf"),
      ParamText(csv_compressionCodec, csv_compressionCodec + "(or codec)", "none",
        Some(List(("none", "none"),
          ("uncompressed", "uncompressed"),
          ("bzip2", "bzip2"),
          ("deflate", "deflate"),
          ("gzip", "gzip"),
          ("lz4", "lz4"),
          ("snappy", "snappy")
        ))),
      ParamText(csv_dateFormat, csv_dateFormat, "yyyy-MM-dd"),
      ParamText(csv_timestampFormat, csv_timestampFormat, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"),
      ParamText(csv_maxColumns, csv_maxColumns, "20480"),
      ParamText(csv_maxCharsPerColumn, csv_maxCharsPerColumn, "-1"),
      ParamText(csv_escapeQuotes, csv_escapeQuotes, "true", Some(booleanOpts)),
      ParamText(csv_maxMalformedLogPerPartition, csv_maxMalformedLogPerPartition, "10"),
      ParamText(csv_quoteAll, csv_quoteAll, "false", Some(booleanOpts))
    )
    case `txt` => List(ParamText(txt_header, "Header in Text file", "true", Some(booleanOpts)),
      ParamText(txt_delimiter, "Delimiter in Text File", ","))
    case `xml` => List(ParamText(xml_rowtag, "Row Tag in XML file", "breakfast_menu"))
    case _ => List()
  }

  def getDataSources = sources

  def getDataSourceParams(d: String) = d match {
    case `hdfs` => List(ParamText(hdfs_namenode, "HDFS Name node", "localhost:9000"), ParamText(hdfs_path, "Path of file in HDFS", ""))
    case `aws` => List(ParamText(aws_id, "S3 access ID", "", None, Some("password")), ParamText(aws_secret, "S3 access secret", "", None, Some("password"))
      , ParamText(aws_location, "S3 bucket location", ""))
    case `adls` => List(ParamText(adls_storage, "Azure Storage Account", "", None, Some("password")), ParamText(adls_container, "Azure Container", ""),
      ParamText(adls_key, "Azure Key", ""), ParamText(adls_filepath, "Azure File path in the container", ""))
    case `gcs` => List(ParamText(gcs_projID, "GCS Project ID", "", None, Some("password")), ParamText(gcs_keyPath, "Path of Key File", "", None, Some("password")),
      ParamText(gcs_bucket, "GCS Bucket Name", ""), ParamText(gcs_path, "File path in the GCS bucket", ""))
    case `lfs` => List(ParamText(lfs_path, "Path on the local file system", ""))
    case _ => List()
  }

  def getFileFormats = fileFormats

  def getFileFormatOptionsForSparkReader(ff: String, z: ZeppelinContext) = {
    val opts = mutable.Map.empty[String, String]
    for (p <- getFileFormatParams(ff)) {
      z.angular(p.name).asInstanceOf[String] match {
        case null => opts += (p.name -> p.default)
        case _ => {
          opts += (p.name -> z.angular(p.name).asInstanceOf[String])
        }
      }
    }
    opts.toMap
  }

  // FIXME [23/10/2019] : This is outdated version. Fix it with new APIs within QueryBuilder class.
  /*def createExternalTable(sc: org.apache.spark.SparkContext, z: ZeppelinContext) = {
    val path = z.get("path").asInstanceOf[String]
    val ds = z.get("dataSource").asInstanceOf[String]
    val ff = z.get("fileFormat").asInstanceOf[String]

    val ss = new org.apache.spark.sql.SnappySession(sc)
    val dropTable = ss.sql(s"""DROP TABLE IF EXISTS ${z.angular("dataset")}""")
    val options =
      s"""path '$path'""" + (ff match {
        case `csv` => s""",$csv_delimiter '${z.angular(csv_delimiter).asInstanceOf[String]}'
                         |,$csv_charset '${z.angular(csv_charset).asInstanceOf[String]}'
                         |,$csv_mode '${"select" + z.angular(csv_mode).asInstanceOf[String]}'
                         |,$csv_header '${"select" + z.angular(csv_header).asInstanceOf[String]}'
                         |,$csv_inferSchema '${"select" + z.angular(csv_inferSchema).asInstanceOf[String]}'""".stripMargin
        case `txt` => s""",$txt_header '${z.angular(txt_header).asInstanceOf[String]}',$txt_delimiter '${z.angular(txt_delimiter).asInstanceOf[String]}'"""
        case `xml` => s""",$xml_rowtag '${z.angular(xml_rowtag).asInstanceOf[String]}' """
        case _ => ""
      })
    val create_query = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${z.angular("dataset")} using $ff OPTIONS($options) """
    val createTable = ss.sql(create_query)
    val schemaTable = ss.table(s"${z.angular("dataset")}")
    (create_query, schemaTable)
  }*/

  def generateTabularSchema(z: ZeppelinContext, df: org.apache.spark.sql.DataFrame, paraID: String): String = {
    val cols = scala.collection.mutable.ListBuffer[(String, String, String)]()
    for (s <- df.schema) {
      cols += ((s.name.toString, s.dataType.toString, s.nullable.toString))
    }
    val s = new StringBuilder()
    val bindCheckboxes = new StringBuilder()
    val unbind = new StringBuilder()
    for (c <- cols) {
      s ++=
          s"""
            <tr>
            <td style="text-align:center"><input type="checkbox" ng-model="${c._1}"/></td>
            <td style="text-align:left">${c._1}</td>
            <td style="text-align:left">${c._2.replaceAll("Type", "").toLowerCase}</td>
            <td style="text-align:center">${c._3}</td>
            </tr>
            """
      bindCheckboxes ++= s"""z.angularBind('${c._1}',${c._1},'${paraID}');"""
      unbind ++= s"""z.angularUnbind('${c._1}','${paraID}');"""
    }

    s"""%angular
        ${tableStyle}
        <table cellspacing="0" rules="all" id="Table1" style="border-collapse: collapse;">
        <tr>
            <th style="width:120px;text-align:center">Import</th>
            <th style="width:120px;text-align:center">Column Name</th>
            <th style="width:120px;text-align:center">Type</th>
            <th style="width:120px;text-align:center">Nullable</th>
        </tr>
        ${s.toString}
        </table>
        <br/>
        ${renderButtons(bindCheckboxes.toString, paraID, unbind.toString(), false)}
        """
  }

  def generateSchemaSelector(z: ZeppelinContext, df: org.apache.spark.sql.DataFrame, paraID: String): String = {
    val cols = new StringBuilder()
    val colUserInput = new StringBuilder()
    val unbind = new StringBuilder()
    for (s <- df.schema) {
      if (z.angular(s.name.toString) != null) {
        cols ++=
            s"""
            <tr>
            <td style="text-align:left">${s.name.toString}</td>
            <td style="text-align:left">
                ${s.dataType.toString.replaceAll("Type", "").toLowerCase}
            </td>
            <td style="text-align:center">${s.nullable.toString}</td>
            <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_name"}"
                 ng-init="${s.name.toString + "_name"}='${s.name.toString}'" value="${s.name.toString}">
                 </input></td>
            <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_dataType"}"
                 ng-init="${s.name.toString + "_dataType"}=
                 '${s.dataType.toString.replaceAll("Type", "").toLowerCase}'"
                 value="${s.dataType.toString.replaceAll("Type", "").toLowerCase}">
                 </input> </td>
            <td><input type="text" class="form-control" ng-model="${s.name.toString + "_nullable"}"
                 ng-init="${s.name.toString + "_nullable"}='${s.nullable.toString}'" value="${s.nullable.toString}">
            </input> </td>
            </tr>"""
        colUserInput ++=
            s"""z.angularBind('${s.name.toString + "_name"}',${s.name.toString + "_name"},'$paraID');
                z.angularBind('${s.name.toString + "_dataType"}',${s.name.toString + "_dataType"},'$paraID');
                z.angularBind('${s.name.toString + "_nullable"}',${s.name.toString + "_nullable"},'$paraID');
                """
        unbind ++=
            s"""z.angularUnbind('${s.name.toString + "_name"}','$paraID');
                z.angularUnbind('${s.name.toString + "_dataType"}','$paraID');
                z.angularUnbind('${s.name.toString + "_nullable"}','$paraID');"""
      }
    }

    s"""%angular
            ${tableStyle}
            <table cellspacing="0" rules="all" id="Table1" style="border-collapse: collapse;">
                <tr>
                    <th style="width:120px;text-align:center">Column Name</th>
                    <th style="width:60px;text-align:center">Type</th>
                    <th style="width:60px;text-align:center">Nullable</th>
                    <th style="width:120px;text-align:center">Import as Column Name</th>
                    <th style="width:60px;text-align:center">Import as Type</th>
                    <th style="width:60px;text-align:center">Import as Nullable</th>
                </tr>
                ${cols.toString}
            </table>
            <br/>
            ${renderButtons(colUserInput.toString, paraID, unbind.toString, false)}
        """
  }
}
