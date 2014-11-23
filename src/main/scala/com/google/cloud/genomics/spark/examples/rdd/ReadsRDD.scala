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
import scala.collection.JavaConversions._
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import com.google.api.services.genomics.model.{Read => ReadModel}
import com.google.api.services.genomics.model.SearchReadsRequest
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.utils.Paginator
import com.google.api.services.genomics.model.CigarUnit
import com.google.cloud.genomics.utils.GenomicsFactory.OfflineAuth

/**
 * A serializable version of the Read.
 * Currently Java Client model objects are not serializable,
 * see https://code.google.com/p/google-api-java-client/issues/detail?id=390
 * for more information.
 */
case class Read(alignedQuality: JList[Integer], cigar: String,
    id: String, mappingQuality: Int, matePosition: Option[Long],
    mateReferenceName: Option[String], fragmentName: String, alignedSequence: String,
    position: Long, readGroupSetId: String, referenceName: String,
    info: Map[String, JList[String]], fragmentLength: Int) extends Serializable

object ReadBuilder {

  val CIGAR_MATCH = Map(
      "ALIGNMENT_MATCH" -> "M",
      "CLIP_HARD" -> "H",
      "CLIP_SOFT" -> "S",
      "DELETE" -> "D",
      "INSERT" -> "I",
      "PAD" -> "P",
      "SEQUENCE_MATCH" -> "=",
      "SEQUENCE_MISMATCH" -> "X",
      "SKIP" -> "N")

  def fromJavaRead(r: ReadModel) = {
    val readKey = ReadKey(r.getAlignment.getPosition.getReferenceName,
        r.getAlignment.getPosition.getPosition)

   val cigar =  r.getAlignment.getCigar.map(cigarUnit =>
     cigarUnit.getOperationLength() +
     CIGAR_MATCH(cigarUnit.getOperation())).mkString("")

    val read = Read(
        r.getAlignedQuality,
        cigar,
        r.getId,
        r.getAlignment.getMappingQuality,
        if (r.containsKey("nextMatePosition"))
          Some(r.getNextMatePosition.getPosition)
        else
          None,
        if (r.containsKey("nextMatePosition"))
          Some(r.getNextMatePosition.getReferenceName)
        else
          None,
        r.getFragmentName,
        r.getAlignedSequence,
        r.getAlignment.getPosition.getPosition,
        r.getReadGroupSetId,
        r.getAlignment.getPosition.getReferenceName,
        r.getInfo.toMap,
        r.getFragmentLength)
        (readKey, read)
  }
}

/**
 * A simple Spark RDD backed by Google Genomics Readstore and populated
 * via the SearchReads API call (https://developers.google.com/genomics/v1beta/reference/reads/search).
 */
class ReadsRDD(sc: SparkContext,
               applicationName: String,
               auth: OfflineAuth,
               readGroupSetIds: List[String],
               readsPartitioner: ReadsPartitioner)
               extends RDD[(ReadKey, Read)](sc, Nil) {

  override val partitioner = Some(readsPartitioner)

  override def getPartitions: Array[Partition] = {
    readsPartitioner.getPartitions(readGroupSetIds)
  }

  override def compute(part: Partition, ctx: TaskContext):
    Iterator[(ReadKey, Read)] = {
    val client = Client(auth).genomics
    val reads = Paginator.Reads.create(client)
    val partition = part.asInstanceOf[ReadsPartition]
    val req = new SearchReadsRequest()
      .setReadGroupSetIds(partition.readGroupSetIds)
      .setReferenceName(partition.sequence)
      .setStart(partition.start)
      .setEnd(partition.end)
    reads.search(req).iterator().map(ReadBuilder.fromJavaRead(_))
  }
}

/**
 * Defines a search range over a named sequence.
 */
case class ReadsPartition(override val index: Int,
                          val readGroupSetIds: List[String],
                          val sequence: String,
                          val start: Long,
                          val end: Long) extends Partition {
}

/**
 * Indexes a mapped read to its partition.
 */
case class ReadKey(sequence: String, position: Long) {
}
