package spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object pagerank {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
    //  .setMaster("local")

    val sc = new SparkContext(conf)
    
   //Read some example file to a test RDD
    val inputFile = sc.textFile(args(0)+"/*.bz2")
    
    var startTime = System.nanoTime();
    // call the wikiParser to parse the data
    val input_list =  inputFile.map {line => Bz2WikiParser.makeGraph(line)}
    
    // we filter out any empty strings returned due to bad data present in the dataset
    val filterNulls = input_list.filter(graphData => graphData != "")
    
    // RDD of array of strings containing pages with their outlinks 
    val splitPairs = filterNulls.map(node=>node.split(":"))
    
    // we make a pairRDD of (pages,List<String> outlinks)and then if the size of the 
    // the list is empty we emit that
    val mapKeyValuePairs =  splitPairs.map{node=>(node(0),
                                      if(node.size>1) node(1).split(", ").toList
                                      else List[String]())}
    
    // to add dangling nodes we emit all the outgoing links present in each pagename
    // and persist into memory so that we can reuse it again while doing pagerank calculations
    val addDanglingNodes = mapKeyValuePairs.map(graphnodes => List((graphnodes._1, graphnodes._2)) :::
                                            graphnodes._2.map(neighbors => (neighbors, List[String]()))).
                                            flatMap(node => node)
                                            .reduceByKey((x,y)=>(x:::y))
                                            .persist()
    var endTime = System.nanoTime();
    println("Time taken for pre-processing "+(endTime-startTime))
    
    val numnodes = addDanglingNodes.count().toDouble
    val initial_pagerank = 1.0/numnodes
    
    // RDD of pagename with their initial pagerank of 1/numnodes
    var pageranks =  addDanglingNodes.map(node => (node._1,initial_pagerank))    
    
    
    // 10 itearations of pagerank
    var startTime2 = System.nanoTime();
    for(i <- 1 until 10){
      
      // to calculate delta, we get all the dangling nodes and then add the pagernak values
      // for the dangling nodes and store it in delta
      var delta = addDanglingNodes.join(pageranks).filter(node => node._2._1.length == 0).
                  reduce((a, b) => (a._1, (a._2._1, a._2._2 + b._2._2)))._2._2 
      
      // we get the contributions of each incoming node and then we add them to get the 
      // the sum of all the contributions from other nodes to the current node
      val contributions = addDanglingNodes.join(pageranks)
                               .flatMap{ case(pagename,(links,rank)) => 
                                var size = links.size
                                // if it is a dangling node output an empty list
                                if(size==0){ 
                                  List()}
                                else{
                                  // else we map the contribution to the outgoing node
                                  links.map(dest=>(dest,rank/size))
                                  }}
                                 // sum all the contributions from the node
                                .reduceByKey((x,y)=>(x+y))
       
      // we get the pages having no inlinks but containing outlinks by checking the original
      // pagerank RDD and the current contribution RDD as these nodes don't get any contributions                          
      val noInlinks = pageranks.subtractByKey(contributions)
      
      // we add thes pages and assign a default pagerank of 0 as it contains no contributions
      val allPages = noInlinks.map(page => (page._1,0.0)).union(contributions)
      
      // we get the updated pagerank values and store it in RDD
      pageranks = allPages.mapValues[Double](v=>((0.15/numnodes)+(0.85*(delta/numnodes))+0.85*v)) 
    }
    var endTime2 = System.nanoTime();
    println("Time taken to calculate pagerank "+(endTime2-startTime2))
    
    var startTime3 = System.nanoTime();
    // we sort the output by passing pagerank as the key and pagename as the value
    val sortoutput = addDanglingNodes.join(pageranks).map(node=>(node._2._2,node._1)).top(100)
    var endTime3 = System.nanoTime();  
    println("Time taken to calculate top100 "+(endTime3-startTime3))
    
    // we save the output in a file
    sc.parallelize(sortoutput,1).saveAsTextFile(args(1))
    
    //Stop the Spark context
    sc.stop
  }
}
