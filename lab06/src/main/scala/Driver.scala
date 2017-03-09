/*
 * DS410 - Lab 06: K-Means Full Version
 * Author: Yuya Jeremy Ong (yjo5006@psu.edu)
 */
import java.io.File
import scala.util.Try
import java.util.Arrays
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions

object Lab05 {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "lab05"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

	// Spark Context & Configurations
	final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
	final val sc = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		// val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		// val sc = new SparkContext(conf)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Load & Parse Iris Dataset
        val lines = sc.textFile("/ds410/lab5/iris.data")
        val samples  = lines.map(line => line.split(",").slice(0,4).map(_.toDouble)).zipWithIndex().map(sample => (sample._2, sample._1))

        // Perform K-Means
        val k = new Kmeans(3, 4)
        k.run(samples, 100)

        val centers = k.centers

		// Generate Output File
        // val writer = new PrintWriter(new File("output.txt"))
		// new_clusters.foreach(x => x._2.foreach(y => writer.write(x._1 + "\t" + y + "\n")))
        // writer.close()
    }
}
