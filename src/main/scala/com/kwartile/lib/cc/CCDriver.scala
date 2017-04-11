/**
  * Copyright (c) 2017 Kwartile, Inc., http://www.kwartile.com
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */

package com.kwartile.lib.cc

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
  * Driver program to run Connected Component.
  */

object CCDriver extends Serializable {

  /**
    *
    * @param args name of the file containing cliques.  One line in the file represent one clique.
    *             e.g.:
    *             1 9 4 5
    *             2 234 23 1
    *             6 3
    */
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("ConnectedComponent")

    val sc = new SparkContext(sparkConf)
    val cliqueFile = args(0)
    val cliquesRec = sc.textFile(args(0))
    val cliques = cliquesRec.map(x => {
      val nodes = x.split("\\s+").map(y => y.toLong).toList
      nodes
    })

    val (cc, didConverge, iterCount) = ConnectedComponent.run(sc, cliques, 20)

    if (didConverge) {
      println("Converged in " + iterCount + " iterations")
      val cc2 = cc.map(x => {
        (x._2, List(x._1))
      })

      /**
        * Get all the nodes in the connected component/
        * We are using a rangePartitioner because the CliquesGenerator produces data skew.
        */
      val rangePartitioner = new RangePartitioner(cc2.getNumPartitions, cc2)
      val connectedComponents =  cc2.reduceByKey(rangePartitioner, (a, b) => {b ::: a})

      //connectedComponents.mapPartitionsWithIndex((index, iter) => {
      //  iter.toList.map(x => (index, x._1, x._2.size)).iterator
      //  }).collect.foreach(println)

      println("connected components")
      connectedComponents.map(x => (x._2.length).toString + " " + x._1 + " " + x._2.sorted.mkString(" ")).saveAsTextFile(cliqueFile + "_cc_out")
    }
    else {
      println("Max iteration reached.  Could not converge")
    }
  }
}
