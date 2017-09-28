## Connected component using Map-reduce on Apache Spark

#### Description
Computing Connected Components of a graph is a well studied problem in Graph Theory and there have been many state of the art algorithms that perform pretty well in single machine environment.  But many of these algorithms perform poorly when we apply them in a distributed setting with hundreds of billions of nodes and edges.  Our first choice was to use, GraphX, the graph computing engine that comes with Apache Spark. Although, GraphX implementation of the algorithm works reasonably well on smaller graphs (we tested up to ~10 million nodes and ~100 million edges), but its performance quickly degraded as we tried to scale to higher numbers. 

We implemented connected component algorithm described in the paper [Connected Components in Map Reduce and Beyond](http://dl.acm.org/citation.cfm?id=2670997).  We liked its approach for two reasons - (1) the algorithm was well suited for our technical stack (Apache Spark on HDFS) (2) more over, other than typical computational complexity, the algorithm also took communication complexity and data skew into account.  The proposed algorithm is iterative but in practice with our dataset and scale, it was able to converge pretty fast with less than ten iterations. In this whitepaper, we describe our implementation.  You can find the full source code of the implementation on github.

---
#### Implementation
We implemented the algorithm on Apache Spark on HDFS using Scala.  We also provide a sample graph generator and a driver program.  You can tune the parameters of this generator to change the characteristics of the generated graph.  The generator saves the generated graph on HDFS.  You can use the driver program to read the generated graph and run the algorithm.  The results of the algorithm is also stored on HDFS.  Alternatively, you can call directly call the API to run the algorithm.

In the implementation, we represent a node by a unique Long number.  Input to the algorithm is a List of Cliques.  A Clique is a list of nodes that are connected together.  For example, given the above example (in figure one), the cliques can be:
```
1:	List(1L, 2L, 3L)
2:	List(3L, 4L)
3:	List(1L, 5L)
4:	List(2L)
5:	List(6L)
6:	List(7L, 8L)
7:	List(6L, 8L)
8:	List(9L)
```

In this case, we have 8 cliques as the input.  As you can see that cliques 1, 2, 3, 4 form one connected component, cliques 5, 6, 7 form second connected component, and clique 8 forms the third connected component.

The main API to drive the algorithm is 
```
ConnectedComponent.run(cliques:RDD[List[Long]], maxIterationCount: Int): (RDD([Long, Long)], Boolean, Int)
```
The API expects you to provide RDD of cliques and maximum number of iterations.  It returns ```RDD[(Long, Long)]``` i.e. a RDD of 2-tuple. The second element of the tuple is the minimum node in a connected component and the first element is another node in the same component.

We first build a List of nodePairs (```RDD[(Long, Long)]```), from the list of given cliques.  We then apply the Large Star and Small Star operations on the list of node pairs.  

We implemented the Large Star algorithm as follows:
```
LargeStar 
Input: List of nodePair(a, b)
Output: List of new nodePairs and change in totalConnectivityChangeCount

1: For every nodePair(a, b) emit nodePair(a, b) and nodePair(b, a).  We call the first element of the tuple-2 as self and the second element as its neighbor
2: Reduce on self to get a list of its neighbors.
3: For every self, apply Large Star operation on its neighbors.  The operation results in a list of new nodePairs.
4: Count the change in connectivity, connectivtyChangeCount, by subtracting the length of the list of neighbors in step 3 from the new list of neighbors in step 4
5: Sum this change for every self to get total change in connectivity, totalConnectivityChangeCount
6: Return the list of new nodePairs and totalConnectivityChangeCount
```
We implemented the Small Star algorithm as follows:
```
SmallStar 
Input: List of nodePair(a, b)
Output: List of new nodePairs and change in totalConnectivityChangeCount

1: For every nodePair(a, b) emit nodePair(a, b) if a > b else emit nodePair(b, a)
2: Rest of the steps are same as that of Large Star.
```
We call the Large Star and Small Star alternatively till the sum of the ```totalConnectivityChangeCount``` becomes zero.  The outputs are RDD of nodePairs, a flag to indicate whether the algorithm converged within the given number of iterations, and count of iterations it took the algorithm to converge.  In our experiments with various datasets, we observed that the algorithm was able to converge within 5 iterations. 

The second element of the resultant nodePair is the minimum node in the connected component.  To get all the nodes in a components, you will need to run reduce operation with second element as the key.  For example, to get all the connected components, you may use the following:
```
val (cc, didConverge, iterCount) = ConnectedComponent.run(cliques, maxIterCount)
If (didConverge) {
	val allComponents = cc.map(x => {
   		val minNode = x._2
		val otherNode = x._1
		(minNode, List(otherNode))
	}).reduceByKey((a, b) => b ::: a)
}
```

---
#### Conclusion
We tested our implementation on various data sizes - scaling up to ~100 billion nodes and ~800 billion edges.  In all the cases, the algorithm converged in no more than 6 iterations.  We indeed had to to try various Spark related configurations, including executor memory size, driver memory size, yarn memory overhead, network timeout, and number of partitions to successfully run the implementation.

We would love to hear your feedback.  Please drop us a note at labs@kwartile.com. 
