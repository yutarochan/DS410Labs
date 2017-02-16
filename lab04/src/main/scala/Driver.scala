/*
 * DS410 - Lab 04: Twitter Hashtag Counts
 * Author: Yuya Jeremy Ong (yjo5006@psu.edu)
 */
import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try

object Lab03 {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "lab04"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Parse Lines, Filter and Extract Tokens
		val lines = sc.textFile("hdfs:/ds410/lab4/CSNNetwork.csv")
		val item  = lines.map(line => line.split(","))
		val string_item = item.filter( i => Try(i(1).toInt).isSuccess && Try(i(2).toInt).isSuccess)

		print(string_item.take(1))

        // Write Output File
		/*
        val writer = new PrintWriter(new File("output.txt"))
        top100.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
        writer.close()
		*/
    }
}
