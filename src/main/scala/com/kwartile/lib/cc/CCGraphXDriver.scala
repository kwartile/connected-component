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

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec


/**
  * GraphX based implementation of Connected Component.
  * you can use this to compare performance of the m/r based algorithm.
  */
object CCGraphXDriver {

  @tailrec
  private def buildEdges(node: Long, neighbors:List[Long], partialPairs: List[Edge[Int]]) : List[Edge[Int]] = {
    if (neighbors.length == 0) {
      if (partialPairs != null)
        List(Edge(node, node, 1)) ::: partialPairs
      else
        List(Edge(node, node, 1))
    } else if (neighbors.length == 1) {
      val neighbor = neighbors(0)
      if (node > neighbor)
        if (partialPairs != null) List(Edge(node, neighbor, 1)) ::: partialPairs else List(Edge(node, neighbor, 1))
      else
      if (partialPairs != null) List(Edge(neighbor, node, 1)) ::: partialPairs else List(Edge(neighbor, node, 1))
    } else {
      val newPartialPairs = neighbors.map(neighbor => {
        if (node > neighbor)
          List(Edge(node, neighbor, 1))
        else
          List(Edge(neighbor, node, 1))
      }).flatMap(x=>x)

      if (partialPairs != null)
        buildEdges(neighbors.head, neighbors.tail, newPartialPairs ::: partialPairs)
      else
        buildEdges(neighbors.head, neighbors.tail, newPartialPairs)
    }
  }

  private def buildEdges(nodes:List[Long]) :  List[Edge[Int]] = {
    buildEdges(nodes.head, nodes.tail, null.asInstanceOf[List[Edge[Int]]])
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("GraphXConnectedComponent")

    val sc = new SparkContext(sparkConf)

    val cliqueFile = args(0)
    val cliquesRec = sc.textFile(args(0))
    val cliques = cliquesRec.map(x => {
      val nodes = x.split("\\s+").map(y => y.toLong).toList
      nodes
    })

    val edges = cliques.map(aClique => {
      buildEdges(aClique)
    }).flatMap(x=>x)

    val graph = Graph.fromEdges(edges, 1)
    val cc = graph.connectedComponents().vertices
    println ("Count of Connected component: " + cc.count)
  }
}
