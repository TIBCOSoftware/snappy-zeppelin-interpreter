package org.apache.zeppelin.interpreter

import org.apache.zeppelin.spark.ZeppelinContext
import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import io.snappydata.SnappyTableStatsProviderService

object QueryBuilder {

  case class ParamText(name: String, desc: String, opts: Option[List[(String, String)]] = None) {

    def render(z: ZeppelinContext) = opts match {
      case Some(select) => z.select(desc,select.toSeq).asInstanceOf[String]
      case _ => z.input(desc).asInstanceOf[String]
    }
  }

  def parseAndRender(pList : List[ParamText], z : ZeppelinContext) = {
    val paramMap = scala.collection.mutable.Map.empty[String, String]
    for (p <- pList)
      paramMap += (p.name -> p.render(z))
    paramMap
  }

  def printHTML(deps : List[String], src : String){
    println(s"""%html <div><h5><span style="font-weight: bold;"> $src</h5> """)
    deps.length match {
      case 0 => println(s"""%html None""")
      case _ => deps.map(d => println(s"""%html $d"""))
     }
  }

  // File formats
  lazy val csv = "CSV"
  lazy val csv_delimiter = "delimiter"
  lazy val csv_encoding = "encoding"
  lazy val csv_mode = "mode"
  lazy val csv_header = "header"
  lazy val csv_inferSchema = "inferSchema"
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
  lazy val hdfs = "HDFS"
  val hdfs_namenode = "namenode"
  lazy val hdfs_path = "path"

  lazy val aws = "AWS"
  lazy val aws_id = "id"
  lazy val aws_secret = "secret"
  lazy val aws_location = "location"
  lazy val aws_accessor = "s3a"

  lazy val lfs = "LFS"
  lazy val lfs_path = "path"

  lazy val gcs = "GCS"
  lazy val gcs_projID = "projectid"
  lazy val gcs_keyPath = "keypath"
  lazy val gcs_bucket = "bucket"
  lazy val gcs_path = "path"
  lazy val gcs_accessor = "gs"

  lazy val adls = "ADLS"
  lazy val adls_storage= "storageAccount"
  lazy val adls_key = "key"
  lazy val adls_container = "container"
  lazy val adls_filepath = "filepath"
  lazy val adls_accessor = "wasb"
  lazy val adls_domain = ".blob.core.windows.net"

  private lazy val fileFormats = List((csv, csv),(prq,prq),(json,json),(orc,orc),(avro,avro),(xml,xml),(txt,txt))
  private lazy val booleanOpts = List(("true", "true"), ("false", "false"))
  private lazy val sources = List((hdfs, "Hadoop File System"),(aws, "Amazon S3"),(lfs, "Local File System"),
                        (gcs, "Google Cloud Storage"),(adls, "Microsoft Azure Store"))

  private val dlSlash = "/"
  private val dlColon = ":"
  private val dlColonSlash = dlColon + dlSlash + dlSlash
  private val dlAtRate = "@"

  def getPathFromParams(params : scala.collection.mutable.Map[String,String], ds : String) = ds match {
    case `hdfs` => hdfs + dlColonSlash + params(hdfs_namenode) + dlSlash + params(hdfs_path)
    case `aws` => aws_accessor + dlColonSlash + params(aws_id) + dlColon + params(aws_secret) + dlAtRate + params(aws_location)
    case `gcs` => gcs_accessor + dlColonSlash + params(gcs_bucket) + dlSlash + params(gcs_path)
    case `adls` => adls_accessor + dlColonSlash + params(adls_container) + dlAtRate + params(adls_storage) +
         adls_domain + dlSlash + params(adls_filepath)
    case `lfs` => params(lfs_path)
    case _ => ""
  }

  def configureDataSourceEnvParams(sc : org.apache.spark.SparkContext,params : scala.collection.mutable.Map[String,String], ds : String) = ds match {
    case `gcs` => {
      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      sc.hadoopConfiguration.set("fs.gs.project.id", params(gcs_projID))
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile",params(gcs_keyPath))
    }
    case `adls` => {
      sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + params(adls_storage) + adls_domain, params(adls_key))
    }
    case _ =>
  }

  def getFileFormatParams(f : String) = f match {
    case `csv` => List(ParamText(csv_encoding, s"Encoding $csv"),
      ParamText(csv_delimiter, s"Delimiter in $csv"),
      ParamText(csv_header, s"Header present in $csv", Some(booleanOpts)),
      ParamText(csv_inferSchema, s"Infer Schema from $csv" , Some(booleanOpts)),
      ParamText(csv_mode, s"Mode for bad records in $csv", Some(List(("DROPMALFORMED", "DROPMALFORMED"), ("FAILFAST", "FAILFAST"))))
    )
    case `txt` => List(ParamText(txt_header,"Header in Text file",Some(booleanOpts)),
                     ParamText(txt_delimiter,"Delimiter in Text File"))
    case `xml` => List(ParamText(xml_rowtag,"Row Tag in XML file"))
    case _ => List()
  }

  def getDataSources = sources

  def getDataSourceParams(d : String) = d match {
    case `hdfs` => List(ParamText(hdfs_namenode,"HDFS Name node"),ParamText(hdfs_path,"Path of file in HDFS "))
    case `aws` => List(ParamText(aws_id,"S3 access ID"),ParamText(aws_secret,"S3 access secret"),ParamText(aws_location,"S3 bucket location"))
    case `adls` => List(ParamText(adls_storage,"Azure Storage Account"),ParamText(adls_container,"Azure Container"),
      ParamText(adls_key,"Azure Key"),ParamText(adls_filepath,"Azure File path in the container"))
    case `gcs` => List(ParamText(gcs_projID,"GCS Project ID"),ParamText(gcs_keyPath,"Path of Key File"),
      ParamText(gcs_bucket,"GCS Bucket Name"),ParamText(gcs_path,"File path in the GCS bucket"))
    case `lfs` => List(ParamText(lfs_path,"Path on the local file system"))
    case _ => List()
  }

  def getDependencyJarNames (d: String) = d match {
    case `adls` => List("hadoop-azure*.jar","azure-storage*.jar")
    case `gcs`  => List("gcs-connector-hadoop2-latest.jar")
    case `avro` => List("Spark-Avro package")
    case `xml`  => List("Spark-XML package")
    case _      => List()
  }

  def getFileFormats = fileFormats

  def createExternalTable(sc : org.apache.spark.SparkContext, z : ZeppelinContext, datasetName : String): Unit ={
    val fParams = z.get("fileParams").asInstanceOf[scala.collection.mutable.Map[String,String]]
    val dParams = z.get("dataSourceParams").asInstanceOf[scala.collection.mutable.Map[String,String]]
    val path = z.get("path").asInstanceOf[String]
    val ds = z.get("dataSource").asInstanceOf[String]
    val ff = z.get("fileFormat").asInstanceOf[String]
    val ss = new org.apache.spark.sql.SnappySession(sc)

    val dropTable = ss.sql(s"""DROP TABLE IF EXISTS $datasetName""")
    val options = s"""path '$path'""" + ( ff match {
      case `csv` => s""",$csv_delimiter '${fParams(csv_delimiter)}'
             |,$csv_encoding '${fParams(csv_encoding)}'
             |,$csv_mode '${fParams(csv_mode)}'
             |,$csv_header '${fParams(csv_header)}'
             |,$csv_inferSchema '${fParams(csv_inferSchema)}'""".stripMargin
      case `txt` => s""",$txt_header '${fParams(txt_header)}',$txt_delimiter '${fParams(txt_delimiter)}'"""
      case `xml` => s""",$xml_rowtag '${fParams(xml_rowtag)}' """
    })
    val create_query = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $datasetName using $ff OPTIONS($options) """
    val createTable = ss.sql(create_query)
    val schemaTable = ss.table(s"$datasetName")
    println("Schema")
    schemaTable.printSchema
    printHTML (List(s"<b>$datasetName created successfully using following query"
      ,s"""<i>$create_query""",s"<b>Row Count : ${schemaTable.count().toString}"),"Result")
    z.show(schemaTable,10)
  }

  def renderDepsUI(ds: String,ff: String, z : ZeppelinContext) : scala.collection.mutable.Map[String, (String,String,String)] = {
    val deps = getDependencyJarNames(ds) ::: getDependencyJarNames(ff)
    val pMap = scala.collection.mutable.Map.empty[String, (String,String,String)]
    deps.foreach(d => {
      val name = z.input(s"Name (ex. MyPackage) for $d").asInstanceOf[String]
      val coordinates = z.input(s"Coordinates (groupId:artifactId:version) for $d").asInstanceOf[String]
      val path = z.input(s"Path (location reachable from LeadNode) for $d").asInstanceOf[String]
      pMap += (d -> (name,coordinates,path))
    })
    pMap
  }
}