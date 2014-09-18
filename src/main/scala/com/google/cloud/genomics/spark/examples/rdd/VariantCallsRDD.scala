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

import java.lang.{Double => JDouble}
import java.util.{List => JList}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import com.google.api.services.genomics.model.{Variant => VariantModel}
import com.google.cloud.genomics.Client

case class AbrevCall(callsetId: String, callsetName: String, 
    genotype: List[Integer]) extends Serializable

case class VariantCalls(calls: Option[Seq[AbrevCall]]) extends Serializable

class VariantCallsRDDBuilder extends RowBuilder[VariantKey, VariantCalls] {
  @Override
  def build(r: VariantModel) = {
    val variantKey = VariantKey(r.getReferenceName(), r.getStart.toLong)

    val calls = if (r.containsKey("calls"))
        Some(r.getCalls().map(
            c => AbrevCall(
                c.getCallSetId, 
                c.getCallSetName, 
                c.getGenotype.toList)))
      else
        None

    val variant = VariantCalls(calls)
    (variantKey, variant)
  }
}

/**
 * A simple Spark RDD backed by Google Genomics VariantStore and
 * populated via the SearchVariants API call
 * (https://developers.google.com/genomics/v1beta/reference/variants/search).
 */
class VariantCallsRDD(sc: SparkContext,
    applicationName: String,
    clientSecretsFile: String,
    dataset: String,
    variantsPartitioner: VariantsPartitioner, 
    maxResults: Int = 50)
    extends RDD[(VariantKey, VariantCalls)](sc, Nil) {

  override val partitioner = Some(variantsPartitioner)

  override def getPartitions: Array[Partition] = {
    variantsPartitioner.getPartitions(dataset)
  }

  override def compute(part: Partition, ctx: TaskContext):
  Iterator[(VariantKey, VariantCalls)] = {
    new VariantsIterator[VariantKey, VariantCalls](Client(applicationName,
            clientSecretsFile).genomics, part.asInstanceOf[VariantsPartition], 
            new VariantCallsRDDBuilder(), maxResults)
  }
}