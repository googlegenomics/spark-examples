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

import java.util.{List => JList}

import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import com.google.api.services.genomics.model.{Read => ReadModel}
import com.google.cloud.genomics.Client

/**
 * A serializable version of the Read.
 * Currently Java Client model objects are not serializable,
 * see https://code.google.com/p/google-api-java-client/issues/detail?id=390
 * for more information.
 */
case class Read(alignedBases: String, baseQuality: String, cigar: String,
    flags: Int, id: String, mappingQuality: Int, matePosition: Int,
    mateReferenceSequenceName: String, name: String, originalBases: String,
    position: Int, readsetId: String, referenceSequenceName: String,
    tags: Map[String, JList[String]], templateLength: Int) extends Serializable
    
object ReadBuilder {
  def fromJavaRead(r: ReadModel) = {
    val readKey = ReadKey(r.getReferenceSequenceName, r.getPosition.toLong)
    
    val read = Read(r.getAlignedBases, 
        r.getBaseQuality, 
        r.getCigar,
        r.getFlags,
        r.getId, 
        r.getMappingQuality,
        r.getMatePosition,
        r.getMateReferenceSequenceName,
        r.getName,
        r.getOriginalBases,
        r.getPosition,
        r.getReadsetId,
        r.getReferenceSequenceName,
        r.getTags.toMap,
        r.getTemplateLength)
        (readKey, read)
  }
}

/**
 * A simple Spark RDD backed by Google Genomics Readstore and populated
 * via the SearchReads API call (https://developers.google.com/genomics/v1beta/reference/reads/search).
 */
class ReadsRDD(sc: SparkContext,
               applicationName: String,
               clientSecretsFile: String,
               readsets: List[String],
               readsPartitioner: ReadsPartitioner) extends RDD[(ReadKey, Read)](sc, Nil) {

  override val partitioner = Some(readsPartitioner)

  override def getPartitions: Array[Partition] = {
    readsPartitioner.getPartitions(readsets)
  }

  override def compute(part: Partition, ctx: TaskContext): Iterator[(ReadKey, Read)] = {
    new ReadsIterator(Client(applicationName, clientSecretsFile).genomics, part.asInstanceOf[ReadsPartition])
  }
}

/**
 * Defines a search range over a named sequence.
 */
case class ReadsPartition(override val index: Int,
                          val readsets: List[String],
                          val sequence: String,
                          val start: Long,
                          val end: Long) extends Partition {
}

/**
 * Indexes a mapped read to its partition.
 */
case class ReadKey(sequence: String, position: Long) {
}
