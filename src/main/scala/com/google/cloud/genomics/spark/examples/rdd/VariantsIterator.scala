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

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList
import com.google.api.services.genomics.Genomics
import com.google.api.services.genomics.model.SearchVariantsRequest
import com.google.api.services.genomics.model.{Variant => VariantModel}
import java.math.BigInteger

/**
 * Performs the search request and provides the resultant variants.
 */
class VariantsIterator[K, V](service: Genomics, part: VariantsPartition,
    builder: RowBuilder[K, V], maxResults: String = "100")
    extends Iterator[(K, V)] {
  // The next page token for the query. If the results span multiple
  // pages, this will hold the next page token. If None, the search is
  // exhausted and so this iterator.
  private var token: Option[String] = Some("")

  // Perform the initial query and establish the iterator.
  private var it = refresh()

  // Executes the search query and returns an iterator to the variants.
  // If the query data is exhausted (i.e. no more pages) the iterator
  // will be empty.
  private def refresh(): Iterator[VariantModel] = {
    token.map { t =>
      println(t)
      val req = new SearchVariantsRequest()
        .setVariantsetId(part.dataset)
        .setContig(part.contig)
        .setMaxResults(new BigInteger(maxResults))
        .setStartPosition(java.lang.Long.valueOf(part.start))
        .setEndPosition(java.lang.Long.valueOf(part.end))

      if (t.length > 0) { req.setPageToken(t) }
      req
    }
      .map { service.variants().search(_).execute() }
      .map { resp =>
        token = resp.getNextPageToken() match {
          case null => None
          case tok => Some(tok)
        }
        resp.getVariants() match {
          case null => None
          case r => Some(asScalaIterator(r.iterator()))
        }
      }
      .flatten
      .getOrElse(List[VariantModel]().iterator())
  }

  override def hasNext: Boolean = {
    if (it.hasNext) {
      true
    } else {
      it = refresh()
      it.hasNext
    }
  }

  override def next(): (K, V) = {
    val r = it.next()
    builder.build(r)
  }
}

trait RowBuilder[K,V] {
  def build(model: VariantModel): (K, V) 
}

