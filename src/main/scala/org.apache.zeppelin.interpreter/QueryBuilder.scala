package org.apache.zeppelin.interpreter

import org.apache.zeppelin.spark.ZeppelinContext
import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql._
import org.apache.spark.SparkContext

object QueryBuilder {

  case class ParamText(name: String, desc: String,default : String , opts: Option[List[(String, String)]] = None, secure : Option[String]  = None)
   /*
   {
      def render(z: ZeppelinContext) = opts match {
        case Some(select) => z.select(desc,select.toSeq).asInstanceOf[String]
        case _ => z.input(desc).asInstanceOf[String]
      }

      def render(): String ={
        (opts,default) match {
          case (Some(opts),Some(default)) => { }
          case (Some(opts),_) => { }
          case (_,Some(default)) => {
            s"""<label for="${name}">${desc}</label> <input type="text" class="form-control" id="${name}" ng-model="${name}" ng-init="${name}='${default}'"}</input>"""
          }
          case _ => { s"""<label for="${name}">${desc}</label>
              <input type="text" class="form-control" id="${name}" ng-model="${name}"}</input>"""}
          }
      }
    }

    def parseAndRender(pList : List[ParamText], z : ZeppelinContext) = {
      val paramMap = scala.collection.mutable.Map.empty[String, String]
      for (p <- pList)
        paramMap += (p.name -> p.render(z))
      paramMap
    }
   */

  def renderParamList(pList : List[ParamText], paraID : String, executeNextParagraph : Boolean = false): Unit = {
    if(!pList.isEmpty) {
      val s = new StringBuilder()
      val bind = new StringBuilder()
      for (p <- pList) {
        (p.opts) match {
          case (Some(x)) => {
            s ++=
                s"""<mat-form-field><label for="select${p.name}" style="width:200px">${p.name}</label>
                    <select matNativeControl required [(ng-model)]="select${p.name}">"""
            for (m <- x)
              s ++= s"""<option value="${m._1}" ${if ((m._2) == p.default) "selected" else ""}>${m._2}</option>"""
            s ++= s"""</select></mat-form-field><br/>"""
            bind ++= s"""z.angularBind('select${p.name}',select${p.name},'${paraID}');"""
          }
          case _ => {
            s ++=
                s"""<label style="width:200px" for="${p.name}">${p.desc}</label> <input type=${
                  (p.secure) match {
                    case Some(y) => y
                    case _ => "text"
                  }
                } class="form-control" id="${p.name}" ng-model="${p.name}" ng-init="${p.name}='${p.default}'" required minlenth="4"}</input><br/>"""
            bind ++= s"""z.angularBind('${p.name}',${p.name},'${paraID}');"""
          }
        }
      }

      println(
        s"""%angular
        <form class="form-inline">
        <div>
            ${s.toString}
            <button type="bind" class="btn btn-primary" ng-click="${bind};${if (executeNextParagraph) s"z.runParagraph('${paraID}')" else ""}">Confirm</button>
        </div>
        </form>
        """)
    }
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

  def getPathFromParams(z : ZeppelinContext, ds : String) = ds match {
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
  def configureDataSourceEnvParams(sc : org.apache.spark.SparkContext,z : ZeppelinContext, ds : String) = ds match {
    case `gcs` => {
      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      sc.hadoopConfiguration.set("fs.gs.project.id", z.angular(gcs_projID).asInstanceOf[String])
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile",z.angular(gcs_keyPath).asInstanceOf[String])
    }
    case `adls` => {
      sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + z.angular(adls_storage).asInstanceOf[String] + adls_domain, z.angular(adls_key).asInstanceOf[String])
    }
    case `aws` => {
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsAccessKeyId",z.angular(aws_id).asInstanceOf[String])
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsSecretAccessKey",z.angular(aws_secret).asInstanceOf[String])
    }
    case _ =>
  }

  def getFileFormatParams(f : String): List[ParamText] = f match {
    case `csv` => List(ParamText(csv_encoding, s"Encoding $csv", "UTF-8"),
      ParamText(csv_delimiter, s"Delimiter in $csv",s","),
      ParamText(csv_header, s"Header present in $csv","true", Some(booleanOpts)),
      ParamText(csv_inferSchema, s"Infer Schema from $csv","true" , Some(booleanOpts)),
      ParamText(csv_mode, s"Mode for bad records in $csv", "DROPMALFORMED", Some(List(("DROPMALFORMED", "DROPMALFORMED"), ("FAILFAST", "FAILFAST"))))
    )
    case `txt` => List(ParamText(txt_header,"Header in Text file","true",Some(booleanOpts)),
                     ParamText(txt_delimiter,"Delimiter in Text File",","))
    case `xml` => List(ParamText(xml_rowtag,"Row Tag in XML file","breakfast_menu"))
    case _ => List()
  }

  def getDataSources = sources

  def getDataSourceParams(d : String) = d match {
    case `hdfs` => List(ParamText(hdfs_namenode,"HDFS Name node","localhost:9000"),ParamText(hdfs_path,"Path of file in HDFS","/hdfs/user/data"))
    case `aws` => List(ParamText(aws_id,"S3 access ID","",None,Some("password")),ParamText(aws_secret,"S3 access secret","",None,Some("password"))
      ,ParamText(aws_location,"S3 bucket location","/bucket/data"))
    case `adls` => List(ParamText(adls_storage,"Azure Storage Account","",None,Some("password")),ParamText(adls_container,"Azure Container",""),
      ParamText(adls_key,"Azure Key",""),ParamText(adls_filepath,"Azure File path in the container",""))
    case `gcs` => List(ParamText(gcs_projID,"GCS Project ID","",None,Some("password")),ParamText(gcs_keyPath,"Path of Key File","",None,Some("password")),
      ParamText(gcs_bucket,"GCS Bucket Name",""),ParamText(gcs_path,"File path in the GCS bucket",""))
    case `lfs` => List(ParamText(lfs_path,"Path on the local file system","/tmp/data/test.file"))
    case _ => List()
  }

/*  def getDependencyJarNames (d: String) = d match {
    case `adls` => List("hadoop-azure*.jar","azure-storage*.jar")
    case `gcs`  => List("gcs-connector-hadoop2-latest.jar")
    case `avro` => List("Spark-Avro package")
    case `xml`  => List("Spark-XML package")
    case _      => List()
  }*/

  def getFileFormats = fileFormats

    def createExternalTable(sc : org.apache.spark.SparkContext, z : ZeppelinContext)= {
      val path = z.get("path").asInstanceOf[String]
      val ds = z.get("dataSource").asInstanceOf[String]
      val ff = z.get("fileFormat").asInstanceOf[String]

      val ss = new org.apache.spark.sql.SnappySession(sc)
      val dropTable = ss.sql(s"""DROP TABLE IF EXISTS ${z.angular("dataset")}""")
      val options =
          s"""path '$path'""" + (ff match {
          case `csv` => s""",$csv_delimiter '${z.angular(csv_delimiter).asInstanceOf[String]}'
                             |,$csv_encoding '${z.angular(csv_encoding).asInstanceOf[String]}'
                             |,$csv_mode '${z.angular(csv_mode).asInstanceOf[String]}'
                             |,$csv_header '${z.angular(csv_header).asInstanceOf[String]}'
                             |,$csv_inferSchema '${z.angular(csv_inferSchema).asInstanceOf[String]}'""".stripMargin
            case `txt` => s""",$txt_header '${z.angular(txt_header).asInstanceOf[String]}',$txt_delimiter '${z.angular(txt_delimiter).asInstanceOf[String]}'"""
            case `xml` => s""",$xml_rowtag '${z.angular(xml_rowtag).asInstanceOf[String]}' """
            case _ => ""
          })
        val create_query = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${z.angular("dataset")} using $ff OPTIONS($options) """
        val createTable = ss.sql(create_query)
        val schemaTable = ss.table(s"${z.angular("dataset")}")
        (create_query,schemaTable)
    }

/*  def verifyParams(dParams: scala.collection.mutable.Map[String,String], fParams : scala.collection.mutable.Map[String,String] ): Boolean ={
    val incompleteDS = dParams.filter(_._2.length == 0 )
    val incompleteFF = fParams.filter(_._2.length == 0)
    val verify = (incompleteDS.keys.size == 0 && incompleteFF.keys.size == 0)
    if(!verify){
      if(incompleteDS.keys.size != 0)
        printHTML(incompleteDS.keys.toList,"Warning : Data Source parameter(s) left blank.")
      if(incompleteFF.keys.size != 0)
        printHTML(incompleteFF.keys.toList,"Warning : File Format parameter(s) left blank.")
    }
    verify
 }*/

/*  def renderDepsUI(ds: String,ff: String, z : ZeppelinContext) : scala.collection.mutable.Map[String, (String,String,String)] = {
    val deps = getDependencyJarNames(ds) ::: getDependencyJarNames(ff)
    val pMap = scala.collection.mutable.Map.empty[String, (String,String,String)]
    deps.foreach(d => {
      val name = z.input(s"Name (ex. MyPackage) for $d").asInstanceOf[String]
      val coordinates = z.input(s"Coordinates (groupId:artifactId:version) for $d").asInstanceOf[String]
      val path = z.input(s"Path (location reachable from LeadNode) for $d").asInstanceOf[String]
      pMap += (d -> (name,coordinates,path))
    })
    pMap
  }*/

  def previewData(ds: String,ff: String,z : ZeppelinContext,sc : org.apache.spark.SparkContext) ={
    val ss = new org.apache.spark.sql.SnappySession(sc)
    val df=ss.read.format(ff).option("header","true").load(z.get("path").asInstanceOf[String])
    df.printSchema
    z.show(df,10)
    val colNames = scala.collection.mutable.ListBuffer[(String,String)]()
    for(s <- df.schema){
      colNames  += ((s.name.toString,s.name.toString))
    }
    z.checkbox("Select columns to be imported in External Table",colNames.toList.toSeq).asInstanceOf[List[String]]
    df
  }

  def generateTabularSchema(z : ZeppelinContext,df : org.apache.spark.sql.DataFrame, paraID : String)
  {
    val cols = scala.collection.mutable.ListBuffer[(String,String,String)]()
    for(s <- df.schema){
      cols  += ((s.name.toString,s.dataType.toString,s.nullable.toString))
    }
    val s = new StringBuilder()
    val bindCheckboxes = new StringBuilder()
    for(c <- cols){
      s ++= s"""
            <tr>
            <td style="text-align:center"><input type="checkbox" ng-model="${c._1}"/></td>
            <td style="text-align:left">${c._1}</td>
            <td style="text-align:left">${c._2.replaceAll("Type","").toLowerCase}</td>
            <td style="text-align:center">${c._3}</td>
            </tr>
            """
      bindCheckboxes ++= s"""z.angularBind('${c._1}',${c._1},'${paraID}'); """
    }

    println(s"""%angular
        <style>
        table, th, td {
            border: 1px solid black;
        }

        th, td {
          padding: 5px;
        }
        </style>
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
        <button type="bind"  class="btn btn-primary" ng-click="$bindCheckboxes">Confirm</button>
        """)
  }

  def generateSchemaSelector(z : ZeppelinContext,df : org.apache.spark.sql.DataFrame, paraID : String)
  {
    val cols = new StringBuilder()
    val colUserInput = new StringBuilder()
    for(s <- df.schema){
      if(z.angular(s.name.toString) == "true"){
        cols ++= s"""
                <tr>
                    <td style="text-align:left">${s.name.toString}</td>
                    <td style="text-align:left">${s.dataType.toString.replaceAll("Type","").toLowerCase}</td>
                    <td style="text-align:center">${s.nullable.toString}</td>
                    <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_name"}" ng-init="${s.name.toString + "_name"}='${s.name.toString}'" value="${s.name.toString}"></input> </td>
                    <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_dataType"}" ng-init="${s.name.toString + "_dataType"}='${s.dataType.toString.replaceAll("Type","").toLowerCase}'" value="${s.dataType.toString.replaceAll("Type","").toLowerCase}"></input> </td>
                    <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_nullable"}" ng-init="${s.name.toString + "_nullable"}='${s.nullable.toString}'" value="${s.nullable.toString}"></input> </td>
                </tr>"""
        colUserInput ++= s"""z.angularBind('${s.name.toString + "_name"}',${s.name.toString + "_name"},'$paraID');
                z.angularBind('${s.name.toString + "_dataType"}',${s.name.toString + "_dataType"},'$paraID');
                z.angularBind('${s.name.toString + "_nullable"}',${s.name.toString + "_nullable"},'$paraID');
                """
      }
    }
    println(s"""%angular
            <style>
            table, th, td {
                border: 1px solid black;
                }

                th, td {
                  padding: 2px;
                }
            </style>
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
            <button type="bind"  class="btn btn-primary" ng-click="$colUserInput">Confirm</button>
        """)
  }
}