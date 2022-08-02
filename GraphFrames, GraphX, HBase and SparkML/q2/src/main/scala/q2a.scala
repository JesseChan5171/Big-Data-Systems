import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.lib.LabelPropagation

object SimpleApp{
   
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def main(args: Array[String]) {
  
    val sc = new SparkContext()

    val cite_edge = GraphLoader.edgeListFile(sc, "edge_list.txt")
    
    val num_vert = cite_edge.vertices.count()
    println(num_vert)
    
    val num_edges = cite_edge.edges.count()
    println(num_edges)

    val vert_lar_in_d = cite_edge.inDegrees.reduce(max)
    println(vert_lar_in_d)
    
    val vert_lar_ouy_d = cite_edge.outDegrees.reduce(max)
    println(vert_lar_ouy_d)
	
	val conn_vert = cite_edge.connectedComponents().vertices
	
	val same_conn = conn_vert.map((v: (Long, Long)) => v._2).distinct.count()
	val conn_num = conn_vert.distinct.count()
	println("bi")
	println(same_conn)
	println(conn_num)
	
	println("bii")
	val st_conn_vert = cite_edge.stronglyConnectedComponents(2).vertices.map(_._2).distinct.count()
	println(st_conn_vert)
	
	println("ci")
	val pr_4300 = PageRank.runParallelPersonalizedPageRank(cite_edge, 10, 0.15, Array(4300,5730))
	
	pr_4300.vertices.top(20)(Ordering.by(_._2(0))).foreach(println)
	
	pr_4300.vertices.top(20)(Ordering.by(_._2(1))).foreach(println)
	
	println("cii")
	val pr_5730 = pr_4300.vertices.top(2000)(Ordering.by(_._2(1)))
	val sub_5730 = cite_edge.subgraph(vpred = (id, attr) => pr_5730.map(_._1) contains id)
	
	val sub_5730_ed_count = sub_5730.edges.count()
	println(sub_5730_ed_count)
	
	println("d")
	val lp_cite = LabelPropagation.run(cite_edge, 50)
	
	val dist_label = lp_cite.vertices.map(_._2).distinct.count()
	println(dist_label);
	
	val max_comm = lp_cite.vertices.map(_._2).map((_,1)).reduceByKey(_+_).reduce(max)
	println(max_comm)
	
  }
}

