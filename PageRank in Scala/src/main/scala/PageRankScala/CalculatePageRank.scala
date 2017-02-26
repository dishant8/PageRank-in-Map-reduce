package PageRankScala


import PageRankJava.Bz2WikiParser
import PageRankJava.Node
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object CalculatePageRank {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("Page Rank in Spark")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Creating the object for the Parser class
    val parsing: Bz2WikiParser = new Bz2WikiParser()

    // Filter to filter out null nodes which represent invalid links from the parser
    def filterRecord(node: Node) : Boolean = { return (node != null) };

    // Calls parser using map which maps each line of RDD to parser method, which validates the html and page name
    // and uses the filter method to filter out the invalid pages
    val graph = sc.textFile(args(0))
      .map(parsing.doParssing(_))
      .filter(filterRecord(_)).cache()

    // Count performs the action of shuffle and sort across various partitions
    val count = graph.count

    // Creates a tupple of page name and its corresponding adjacency list from the node object
    val tupleRdd = graph.map(node => (node.getPageName, node.getAdjacencylist.toList))

    //  Uses flatMapValues to iterate over each element of adjacency list, calculates its contribution and
    val initialContribution = tupleRdd.flatMapValues(list => list.toList.map(elementInList
    => (elementInList, ((1.0/count)/list.size)))).values.reduceByKey(_+_)


  // Performing fullOuterJoin on the tupple RDD i.e. the full graph containing the adjacnency 
  //elements of each page and tupple of initialContribution i.e. pageName and its contribution value will return a RDD
    val afterJoin = tupleRdd.fullOuterJoin(initialContribution)

    // It removes the Options i.e the Some and None generated after the fullOuterJoin

    val removeSomeAndNone = afterJoin.map(row => (row._1, (row._2._1.getOrElse(List()), (row._2._2.getOrElse(0.0)))))
    val pageAndAdjacent = removeSomeAndNone.map(eachLine => (eachLine._1, eachLine._2._1)).cache
    var pageAndValue = removeSomeAndNone.map(eachLine => (eachLine._1, eachLine._2._2))
    val newCount = removeSomeAndNone.count();

    // Function to filter and get the adjacency list which are empty and calculate the 
    // dangling value for all such dangling node to calculate the pagerank.
    def filterForDangling(arrayForAdjacency: List[String]) : Boolean = { return (arrayForAdjacency.size == 0)};

    // Ten iterations 
    for(x <- 1 to 10){
        // Creates a join to get the entire graph with adjacency list and the page rank value. 
        // pageAndAdjacent will be same across all the iterations hence cached it so that it is not calculated everytime  
      val afterLeftJoin = pageAndAdjacent.join(pageAndValue)

      // After getting the pageRankvalue in each of the new iterations, calculating the contribution 
      // to its adjacency list by calculating newPageRankValue/size of adjacency.
      val contribution = afterLeftJoin
          .flatMapValues(eachLine => eachLine._1
          .map(elementInList
          => (elementInList, (eachLine._2 / eachLine._1.size)))).values.reduceByKey(_+_)


      // Calculating the dangling value for the dangling nodes from the last iterations. Thus applying filter on afterLeftJoin 
      // which contains the graph with adjacency list and new pageRank value from the last iteration.     
      val danglingValue = afterLeftJoin.filter(row => filterForDangling(row._2._1))
        .map(row => row._2._2).fold(0) { (z,i) =>
        z + i
      }

      //After getting the contributions, considering new page rank values and the 
      // dangling values and applying the formula to calculate the new page rank value
      pageAndValue = contribution.map(eachLine => (eachLine._1, (0.15/newCount) + (0.85 * (eachLine._2 + (danglingValue/newCount)))))
    }

    // pageAndValue contains the tuple of (pageName, PageRankValue), SortBy return RDD sorted by pageRankValues. 
    // Getting the 100 values out of it and daving it to text file. 

    sc.parallelize(pageAndValue.sortBy(_._2, false, 1).take(100), 1).map(eachLine => (eachLine._1,eachLine._2))
      .saveAsTextFile(args(1))

    sc.stop()
  }


}