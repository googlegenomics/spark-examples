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
import com.google.api.services.genomics.model.{ Variant => VariantModel,
      Call => CallModel, SearchVariantsRequest, SearchVariantsResponse }
import com.google.cloud.genomics.Client
import org.apache.spark.{ Logging, Partition, Partitioner,
      SparkContext, TaskContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

/**
 * A serializable version of the Variant.
 * Currently Java Client model objects are not serializable, see
 * https://code.google.com/p/google-api-java-client/issues/detail?id=390
 * for more information.
 */

// Convert from a Map to an Object
case class Call(model: Map[String, Any]) extends Serializable {
  def callsetName: String = model("callsetName").asInstanceOf[String]
  def callsetId: String = model("callsetId").asInstanceOf[String]
  def genotype: List[String] = model("genotype").asInstanceOf[List[String]]
  def genotypeLikelihood: List[Double] =
      model("genotypeLikelihood").asInstanceOf[List[Double]]
  def phaseset: String = model("phaseset").asInstanceOf[String]
  def info: Map[String, List[String]] =
      model("info").asInstanceOf[Map[String, List[String]]]
}
case class Variant(model: Map[String, Any]) extends Serializable {
  def contig: String = model("contig").asInstanceOf[String]
  def id: String = model("id").asInstanceOf[String]
  def names: List[String] = model("names").asInstanceOf[List[String]]
  def position: Long = model("position").asInstanceOf[Long]
  def end: Long = model("end").asInstanceOf[Long]
  def referenceBases: String = model("referenceBases").asInstanceOf[String]
  def alternateBases: List[String] =
      model("alternateBases").asInstanceOf[List[String]]
  def info: Map[String, List[String]] =
      model("info").asInstanceOf[Map[String, List[String]]]
  def created: Long = model("created").asInstanceOf[Long]
  def datasetId: String = model("datasetId").asInstanceOf[String]
  def calls: List[Call] = model("calls").asInstanceOf[List[Call]]
}

/**
 * A simple Spark RDD backed by Google Genomics VariantStore and
 * populated via the SearchVariants API call
 * (https://developers.google.com/genomics/v1beta/reference/variants/search).
 */
class VariantsRDD(sc: SparkContext,
    applicationName: String,
    clientSecretsFile: String,
    dataset: String,
    variantsPartitioner: VariantsPartitioner)
    extends RDD[(VariantKey, Variant)](sc, Nil) {

  override val partitioner = Some(variantsPartitioner)

  override def getPartitions: Array[Partition] = {
    variantsPartitioner.getPartitions(dataset)
  }

  override def compute(part: Partition, ctx: TaskContext):
  Iterator[(VariantKey, Variant)] = {
    new VariantsIterator(Client(applicationName,
            clientSecretsFile).genomics, part.asInstanceOf[VariantsPartition])
  }
}

/**
 * Defines a search range over a contig.
 */
case class VariantsPartition(override val index: Int,
                          val dataset: String,
                          val contig: String,
                          val start: Long,
                          val end: Long) extends Partition {
}

/**
 * Indexes a variant to its partition.
 */
case class VariantKey(contig: String, position: Long) {
}
