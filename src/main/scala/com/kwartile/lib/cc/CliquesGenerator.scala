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

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Generates test data.  Saves the data in HDFS.
  * Every line of the file represents a clique (subgraph)
  *
  */

object CliquesGenerator extends Serializable {

  /**
    * Generate test data in parallel. Useful when you want to create large dataset to test
    * @param sc spark context
    * @param cliquesCount number clique counts you want to generate
    * @param maxNodesPerComponent maximum number of nodes per clique.
    * @param numPartition partition count
    * @return RDD of the cliques
    */
  def generateInParallel(sc: SparkContext, cliquesCount: Long, maxNodesPerComponent: Int, numPartition: Int):
                (RDD[List[List[Long]]], RDD[List[List[Long]]]) = {

    require(cliquesCount > 1000 && cliquesCount % 1000 == 0 && maxNodesPerComponent > 0 && maxNodesPerComponent < 1000)
    val partitionCount = Math.min(numPartition, cliquesCount / numPartition)
    val numElementsPerPartition = cliquesCount / numPartition
    val distData  = sc.parallelize(0 to partitionCount.toInt, partitionCount.toInt)
      .mapPartitionsWithIndex((x, _) => {
        val cliqueBuffer = new ListBuffer[List[Long]]
        val rand = new scala.util.Random(19345)
        val partRange = (x * numElementsPerPartition to numElementsPerPartition * x + numElementsPerPartition - 1).toList
        val connectedComponentsBuffer = new ListBuffer[List[Long]]
        partRange.map(i => {
          val allComponentsBuffer = new ListBuffer[List[Long]]
          val idRange = (1000L * i to 1000L * i + rand.nextInt(maxNodesPerComponent) + 2).toList
          val numComponents = 2 + rand.nextInt(Math.min(maxNodesPerComponent, 100))
          val numNodesPerComponent = 2 + rand.nextInt(Math.min(maxNodesPerComponent, 20))
          var connectingElement = idRange.head
          for (i <- 0 to numComponents - 1) {
            val comp = (connectingElement :: Random.shuffle(idRange).take(numNodesPerComponent)).toSet.toList
            cliqueBuffer += comp
            allComponentsBuffer += comp
            connectingElement = Random.shuffle(comp).head
          }
          val cc = allComponentsBuffer.toList.flatten.toList.distinct
          connectedComponentsBuffer += cc
        })
        List((cliqueBuffer.toList, connectedComponentsBuffer.toList)).iterator

      }).cache
    val cliques = distData.map(x => x._1)
    val cc = distData.map(x => x._2)
    distData.unpersist(false)
    (cliques, cc)
  }

  /**
    * Generate test data
    * @param sc spark context
    * @param cliquesCount number clique counts you want to generate
    * @param maxNodesPerComponent maximum number of nodes per clique.
    * @return
    */
  def generate(sc: SparkContext, cliquesCount: Int, maxNodesPerComponent: Int): (List[List[Long]], List[List[Long]]) = {
    
    require(cliquesCount > 0 && maxNodesPerComponent > 0 && maxNodesPerComponent < 1000)

    val cliqueBuffer = new ListBuffer[List[Long]]
    val rand = new scala.util.Random(19345) 
    val connectedComponentsBuffer = new ListBuffer[List[Long]]
    for (i <- 0 to cliquesCount) {
      val allComponentsBuffer = new ListBuffer[List[Long]]
      val idRange = (1000L * i to 1000L * i + rand.nextInt(maxNodesPerComponent) + 2).toList
      val numComponents = 2 + rand.nextInt(Math.min(maxNodesPerComponent, 100))
      val numNodesPerComponent = 2 + rand.nextInt(Math.min(maxNodesPerComponent, 20))
      var connectingElement = idRange.head
      for (i <- 0 to numComponents-1) {
        val comp = (connectingElement :: Random.shuffle(idRange).take(numNodesPerComponent)).toSet.toList
        cliqueBuffer += comp
        allComponentsBuffer += comp
        connectingElement = Random.shuffle(comp).head
      }
      val cc = allComponentsBuffer.toList.flatten.toList.distinct
      connectedComponentsBuffer += cc
    }   
    (cliqueBuffer.toList, connectedComponentsBuffer.toList)
  } 
  
  def main(args: Array[String]) = {

    val argsOptions = new Options
    argsOptions.addOption("cliquesCount", "cliquesCount", true, " Count of cliques")
    argsOptions.addOption("maxNodesPerComponent", "maxNodesPerComponent", true, " maximum number of nodes per component")
    argsOptions.addOption("outputFile", "outputFile", true, " name of the output file")
    argsOptions.addOption("numPartition", "numPartition", true, " number of partitions")

    val clParser = new BasicParser
    val clArgs = clParser.parse(argsOptions, args)
    val cliquesCount = clArgs.getOptionValue("cliquesCount").toLong
    val maxNodesPerComponent = clArgs.getOptionValue("maxNodesPerComponent").toInt
    val outputFile = clArgs.getOptionValue("outputFile")
    val numPartition = clArgs.getOptionValue("numPartition").toInt

    val sparkConf = new SparkConf().setAppName("CliqueGenerator")

    val sc = new SparkContext(sparkConf)
    val (cliques, cc) = CliquesGenerator.generateInParallel(sc, cliquesCount, maxNodesPerComponent, numPartition)

    val cliquesStr = cliques.map(x => x.map(i => i.mkString(" "))).flatMap(x=>x)
    cliquesStr.saveAsTextFile(outputFile)
    val ccStr = cc.map(x => x.map(i => i.mkString(" "))).flatMap(x=>x)
    ccStr.saveAsTextFile(outputFile + "_cc")
  }
}
