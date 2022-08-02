import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object SimpleApp {
   
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def main(args: Array[String]) {
  
    val sc = new SparkContext()

    val cite_edge = GraphLoader.edgeListFile(sc, "edge_list.txt")
    
    val num_vert = cite_edge.vertices.count()
    println(Num_vert)
    
    val num_edges = cite_edge.edges.count()
    println(num_edges)

    val vert_lar_in_d = cite_edge.inDegrees.reduce(max)
    println(vert_lar_in_d)
    
    val vert_lar_ouy_d = cite_edge.outDegrees.reduce(max)
    println(vert_lar_ouy_d)
    
  }

}

