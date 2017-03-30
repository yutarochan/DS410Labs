package lab7

import java.io.File
import java.util.Arrays
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source

object Demo {
    // Application Specific Variables
    private final val SPARK_MASTER = "yarn-client"
    private final val APPLICATION_NAME = "lab7"
    private final val DATASET_PATH_PUBMED = "/tmp/pubmed.csv"

    // HDFS Configuration Files
    private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
    private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
    final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
    final val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {
        // Configure HDFS
        val configuration = new Configuration();
        configuration.addResource(CORE_SITE_CONFIG_PATH);
        configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Print Usage Information
        System.out.println("\n----------------------------------------------------------------\n")
        System.out.println("Usage: spark-submit [spark options] lab7.jar [exhibit]")
        System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
        System.out.println("\n----------------------------------------------------------------\n");

        // Exhibit: KMeans Clustering
        if(args(0) == "kmeans") {
            // Process Data from File
            val lines = sc.textFile(FileSystem.get(configuration).getUri + DATASET_PATH_PUBMED)
            val papers = Helper.parseData(lines)

            // Generate Feature Vectors
            val featureVectors = FeatureExtraction.constructFeatureVectorsFromPapers(papers).cache()

            // Compute Clusters
            val start = System.nanoTime
            val clustersOfPapers = new KMeansClustering(15, 100).clusterPapers(featureVectors)
            val end = System.nanoTime

            // Output Cluster
            val results = clustersOfPapers.map(x => (x, 1)).groupByKey().map(x => (x._1, x._2.sum)).collect()
            var writer = new PrintWriter(new File("centers_15.txt"))
            for (i <- results) {
                writer.write(i._1 + "\t" + i._2 + "\n")
            }
            writer.close()

            for (p <- clustersOfPapers) println(p)

            // Record Output Time
            println((end - start) / 10e9 + "s")
        }
    }
}
