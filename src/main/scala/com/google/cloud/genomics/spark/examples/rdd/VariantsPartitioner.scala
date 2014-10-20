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
 * Describes partitions for a set of contigs and their ranges.
 */
class VariantsPartitioner(variants: Map[String, (Long, Long)],
    splitter: ReferenceSplitter) extends Partitioner {
  // Maps contig name to partition count.
  final val parts = TreeMap[String, Int]() ++
    (variants.map(kv => (kv._1, splitter.getNumberOfSplits(kv._2._2 - kv._2._1))).toMap)

  // Total partition count.
  final val count = parts.foldLeft(0)(_ + _._2)

  // Maps contig name to starting parition.
  final val steps = parts.tail.scanLeft((parts.head._1, 0))((a, b)
      => (b._1, a._2 + b._2))

  override def getPartition(key: Any): Int = {
    val rk = key.asInstanceOf[VariantKey]
    val contig = rk.contig
    val len = {
      val range = variants(contig)
      range._2 - range._1
    }
    (steps(contig) + ((parts(contig) - 1) / (len / rk.position))).toInt
  }

  override def numPartitions: Int = count

  // Generates all partitions for all mapped variants in the contig space.
  def getPartitions(variantSetId: String): Array[Partition] = {
    variants.map { kv =>
      val (name: String, range: (Long, Long)) = kv
      var idx = steps(name)
      val n = parts(name)
      val span = (range._2 - range._1) / n
      (0 until n).map { i =>
        val start = range._1 + (i * span)
        val p = VariantsPartition(idx, variantSetId, name, start, start + span)
        idx += 1
        p
      }.toArray
    }.flatten.toArray
  }
}

/**
 * Used to determine the number of parts in which a reference should be splitted.
 *
 * @param referenceSize the size of the reference to be split.
 * @return the number of parts into which this reference should be splitted.
 */
trait ReferenceSplitter {
  def getNumberOfSplits(referenceSize: Long): Int
}

/**
 * Split a reference in a constant number of parts no matter the reference size.
 * If the number of requested splits is smaller than the reference size, return
 * the reference size.
 */
case class FixedSplitsPerReference(numSplits: Int) extends ReferenceSplitter {
  def getNumberOfSplits(referenceSize: Long): Int = math.min(
      referenceSize, numSplits).toInt
}

/**
 * Split a reference using a constant number of bases per reference.
 * If the number of bases is smaller than the reference size return one split.
 */
case class FixedBasesPerReference(numBases: Int) extends ReferenceSplitter {
  def getNumberOfSplits(referenceSize: Long): Int =  {
    val bases = math.min(referenceSize, numBases)
    math.ceil(referenceSize / bases.toDouble).toInt
  }
}
