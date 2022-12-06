import org.apache.spark.graphx._

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

import org.apache.spark.rdd._



//Find the number of vertices that are reachable from a node

object Problem2 {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("minValue").setMaster("local")

    val sc = new SparkContext(conf)

    

    val fileName = args(0)

    val outputFolder = args(1)

    

    //Using lab material to construct a graph

    val edges = sc.textFile(fileName)   

   

    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 0.0))	

    val graph = Graph.fromEdges[Double,Double](edgelist, 0)



    //for each vertex initialize a empty list to store reachable vertex ids

    def vprog(id: VertexId, value: List[VertexId], message: List[VertexId]) : List[VertexId] = message

    

    //Update each vertexId with neighboring vertexIds and produce an Iterator to send reachable vertex to next interation

    def sendMsg(triplet: EdgeTriplet[List[VertexId],Double]) : Iterator[(VertexId, List[VertexId])] = {

      val Neighbors = (triplet.dstId :: triplet.dstAttr).filter(_ != triplet.srcId)

  

      if (Neighbors.intersect(triplet.srcAttr).length == Neighbors.length)

        

        Iterator.empty

      else

        Iterator((triplet.srcId, Neighbors))

    }

    

    //combine both VertexId lists as nodes reachable from a reachable vertex is reachable from source vertex.

    def mergeMsg(a: List[VertexId], b: List[VertexId]) : List[VertexId] = (a ::: b).distinct



    val initialMsg = List.empty[VertexId]



    //run pregel  

    val result = graph

      .mapVertices((_,_) => List.empty[VertexId])

      .pregel(

        initialMsg,

        activeDirection = EdgeDirection.Out

      )(vprog, sendMsg, mergeMsg)

     

     //format answer to project spec. Note the number of reachable nodes is the length of the vertex list.

     val answer = result.vertices.sortBy(_._1).map(x => s"${x._1}:${x._2.length}").collect()

     sc.parallelize(answer,1).saveAsTextFile(outputFolder)



   sc.stop

  }

}
