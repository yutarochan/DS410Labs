/*
 * DS410 - Lab 04: Network Clustering Coefficient
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

object Lab04 {
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

		// Parse Edge ID Values
		val int_item = string_item.map( cs => (cs(1).toInt, cs(2).toInt)).filter( cs => cs._1 != cs._2)

		// Sort Edge Values
		val edge_increase = int_item.map(cs => (if (cs._1 < cs._2) (cs._1, cs._2); else (cs._2, cs._1))).distinct()
		val edge_decrease = int_item.map(cs => (if (cs._1 < cs._2) (cs._2, cs._1); else (cs._1, cs._2))).distinct()

		// Join Edge Inc & Dec
		val two_edge = edge_increase.join(edge_decrease).map(cs => (cs._2, cs._1))

		// Formulate Triangles
		val extended_edge_decrease = edge_decrease.map(cs => (cs, 1))
		val triangle = extended_edge_decrease.join(two_edge)

		// Aggregate Edges by Node ID
		val edgeAgg = triangle.map(x => (x._2._2, x._1)).distinct().groupByKey.mapValues(_.toList)

		// Compute Triangle Per Node
		val triCount = edgeAgg.map(x => (x._1, x._2.length))

		// Count List of Neighbors Per Node
		val nodeCount = edgeAgg.map(x => (x._1, (x._2.flatten {case (a,b) => List(a,b)}).distinct.length ))

		// Aggregate and Compute Clustering Coefficient
		// (Node_ID, (Triangle Count, Neighbor Count))

		// TODO: Compute the Clustering Coefficient!!!!!
		val result = triCount.join(nodeCount).sortByKey(false).map(x => (x._1, x._2._1, (x._2._1 / ((x._2._2 * (x._2._2 - 1) * (x._2._2 - 2))/6.0).toDouble)).collect()

		// Generate Output File
        val writer = new PrintWriter(new File("output.txt"))
        result.foreach(x => writer.write(x._1 + "\t" + x._2 + "\t" + x._3 + "\n"))
        writer.close()
    }
}
