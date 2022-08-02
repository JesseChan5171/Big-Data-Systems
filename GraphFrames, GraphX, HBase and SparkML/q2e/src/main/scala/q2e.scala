import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.lib.LabelPropagation

object SimpleApp2{
   
  def main(args: Array[String]) {
  
    val sc = new SparkContext()

    val dag = GraphLoader.edgeListFile(sc, "dag_edge_list.txt")
    val init_g = dag.mapVertices((_,_) => 0)
	
    val sssp = init_g.pregel(0)(
        (id, dist, newDist) => math.max(dist, newDist), // Vertex Program
        triplet => {  // Send Message
        if (triplet.srcAttr + 1 > triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
		Iterator.empty
        }
      },
      (a, b) => math.max(a, b) // Merge Message
    )
    
    println(sssp.vertices.collect.mkString("\n"))
  }

}

