/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics.spark.examples.rdd

import org.apache.spark.{ Logging, Partition, Partitioner }
import collection.immutable.TreeMap

/**
 * Describes partitions for a set of sequences and their ranges.
 */
class ReadsPartitioner(sequences: Map[String, (Long, Long)],
                       splitter: SequenceSplitter) extends Partitioner {
  // Maps sequence name to partition count.
  final val parts = TreeMap[String, Int]() ++
    (sequences.map(kv => (kv._1, splitter.splits(kv._2._2 - kv._2._1))).toMap)

  // Total partition count.
  final val count = parts.foldLeft(0)(_ + _._2)

  // Maps sequence name to starting parition.
  final val steps = parts.tail.scanLeft((parts.head._1, 0))((a, b) => (b._1, a._2 + b._2))

  def getPartition(key: Any): Int = {
    val rk = key.asInstanceOf[ReadKey]
    val seq = rk.sequence
    val len = {
      val range = sequences(seq)
      range._2 - range._1
    }
    (steps(seq) + ((parts(seq) - 1) / (len / rk.position))).toInt
  }

  def numPartitions: Int = count

  // Generates all partitions for all mapped reads in the sequence space.
  def getPartitions(readsets: List[String]): Array[Partition] = {
    sequences.map { kv =>
      val (name: String, range: (Long, Long)) = kv
      var idx = steps(name)
      val n = parts(name)
      val span = (range._2 - range._1) / n
      (0 until n).map { i =>
        val start = range._1 + (i * span)
        val p = ReadsPartition(idx, readsets, name, start, start + span)
        idx += 1
        p
      }.toArray
    }.flatten.toArray
  }
}

/**
 * Used to describe how a sequence should be partitioned.
 */
trait SequenceSplitter {
  def splits(sequenceLength: Long): Int
}

/**
 * Used to split a sequence into a fixed number of partitions.
 */
case class FixedSplits(numSplits: Int) extends SequenceSplitter {
  def splits(sequenceLength: Long): Int = math.min(sequenceLength, numSplits).toInt
}

/**
 * Used to split a sequence into a number of similarly sized partitions based on
 * the length and some known (or estimated) parameters.
 */
case class TargetSizeSplits(readLength: Int, readDepth: Int, readSize: Int, partitionSize: Long) extends SequenceSplitter {
  def splits(sequenceLength: Long): Int = {
    1 + (((sequenceLength / readLength) * readDepth * readSize).toLong / (partitionSize + 1)).toInt
  }
}
