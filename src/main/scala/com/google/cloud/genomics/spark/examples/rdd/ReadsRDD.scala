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

import com.google.api.services.genomics.Genomics
import com.google.api.services.genomics.model.{ Read => ReadModel, SearchReadsRequest, SearchReadsResponse }
import com.google.cloud.genomics.Client
import org.apache.spark.{ Logging, Partition, Partitioner, SparkContext, TaskContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

/**
 * A serializable version of the Read.
 * Currently Java Client model objects are not serializable,
 * see https://code.google.com/p/google-api-java-client/issues/detail?id=390
 * for more information.
 */
case class Read(model: Map[String, Any]) extends Serializable {
  def alignedBases: String = model("alignedBases").asInstanceOf[String]
  def baseQuality: String = model("baseQuality").asInstanceOf[String]
  def cigar: String = model("cigar").asInstanceOf[String]
  def flags: Int = model("flags").asInstanceOf[Int]
  def id: String = model("id").asInstanceOf[String]
  def mappingQuality: Int = model("mappingQuality").asInstanceOf[Int]
  def matePosition: Int = model("matePosition").asInstanceOf[Int]
  def mateReferenceSequenceName: String = model("mateReferenceSequenceName").asInstanceOf[String]
  def name: String = model("name").asInstanceOf[String]
  def originalBases: String = model("originalBases").asInstanceOf[String]
  def position: Int = model("position").asInstanceOf[Int]
  def readsetId: String = model("readsetId").asInstanceOf[String]
  def referenceSequenceName: String = model("referenceSequenceName").asInstanceOf[String]
  def tags: Map[String, List[String]] = model("tags").asInstanceOf[Map[String, List[String]]]
  def templateLength: Int = model("templateLength").asInstanceOf[Int]
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
