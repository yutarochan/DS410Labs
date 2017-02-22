/*
 * DS410 - Lab 05: Network Clustering Coefficient
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

object Lab05 {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "lab05"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    def initialCluster(nb_cluster:Int, nb_feature:Int) : Array[(Int, Array[Double])] = {
        var clusters = ofDim[(Int, Array[Double])](nb_cluster)
        for (i <- 0 to nb_cluster-1) {
            clusters(i) = (i, Array.fill(nb_feature){scala.util.Random.nextDouble()} )
        }
        return clusters
    }

    def Distance(a:Array[Double], b:Array[Double]) : Double = {
        assert(a.length == b.length, "Distance(): features dim does not match.")
        var dist = 0.0
        for (i <- 0 to a.length-1) {
            dist = dist + math.pow(a(i) - b(i), 2)
        }
        return math.sqrt(dist)
    }

    // def step(Array[(Int, Array[Double])]) : Array[(Int, Array[Double])] = {}

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Cluster Parameters
        val nb_cluster = 3
        val nb_feature = 4

        // Import and Parse Dataset
        val lines = sc.textFile("/ds410/lab5/iris.data")
        val samples  = lines.map(line => line.split(",").slice(0,4).map(_.toDouble)).zipWithIndex().map(sample => (sample._2, sample._1))

        // Broadcast Centroid Parameters to Cluster
        //val clusters = sc.broadcast(initialCluster(3, 4))
        val clusters = sc.broadcast(Array((0,Array(5.1,3.5,1.4,0.2)), (1,Array(4.9,3.0,1.4,0.2)), (2,Array(4.7,3.2,1.3,0.2))))

        // Complete this line:
        // Expected output structure: (sampleID, (clusterID, Distance(sample, cluster))
        val dist = samples.map(samp => clusters.value.map(clus => (samp._1, (clus._1, Distance(samp._2, clus._2))) ))

        val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1))
        // (sampleID, clusterID)

        var  new_clusters = Array.ofDim[(Int, Array[Double])](nb_cluster)
        for (i <- 0 to nb_cluster-1) {
            val sample_in_cluster = samples.join(labels.filter(i==_._2))
            val total_number = sample_in_cluster.count
            if (total_number != 0) {
                var tmp = sample_in_cluster.map(sample => sample._2._1).reduce((a, b) => a.zip(b).map{ case (x, y) => x + y })
                tmp = tmp.map( a => a/total_number.toDouble)
                new_clusters(i) = (i, tmp)
            }
            else {
                new_clusters(i) = (i, samples.takeSample(false, 1)(0)._2)
            }
        }

		// Generate Output File
        /*
        val writer = new PrintWriter(new File("output.txt"))
        result.foreach(x => writer.write(x._1 + "\t" + x._2 + "\t" + x._3 + "\n"))
        writer.close()
        */
    }
}
