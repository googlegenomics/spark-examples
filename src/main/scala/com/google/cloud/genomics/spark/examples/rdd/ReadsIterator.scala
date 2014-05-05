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

import collection.JavaConversions._

import com.google.api.services.genomics.Genomics
import com.google.api.services.genomics.model.{ Read => ReadModel, SearchReadsRequest }
import org.apache.spark.{ Logging, Partition }

/**
 * Performs the search request and provides the resultant reads.
 */
class ReadsIterator(service: Genomics, part: ReadsPartition) extends Iterator[(ReadKey, Read)] {
  // The next page token for the query. If the results span multiple pages, this will
  // hold the next page token. If None, the search is exhausted and so this iterator.
  private var token: Option[String] = Some("")

  // Perform the initial query and establish the iterator.
  private var it = refresh()

  // Executes the search query and returns an iterator to the reads.
  // If the query data is exhausted (i.e. no more pages) the iterator will be empty.
  private def refresh(): Iterator[ReadModel] = {
    token.map { t =>
      var req = new SearchReadsRequest()
        .setReadsetIds(part.readsets)
        .setSequenceName(part.sequence)
        .setSequenceStart(java.math.BigInteger.valueOf(part.start))
        .setSequenceEnd(java.math.BigInteger.valueOf(part.end))

      if (t.length > 0) { req.setPageToken(t) }
      req
    }
      .map { service.reads().search(_).execute() }
      .map { resp =>
        token = resp.getNextPageToken() match {
          case null => None
          case tok => Some(tok)
        }
        resp.getReads() match {
          case null => None
          case r => Some(asScalaIterator(r.iterator()))
        }
      }
      .flatten
      .getOrElse(List[ReadModel]().iterator())
  }

  override def hasNext: Boolean = {
    if (it.hasNext) {
      true
    } else {
      it = refresh()
      it.hasNext
    }
  }

  override def next(): (ReadKey, Read) = {
    val r = it.next()
    (ReadKey(r.getReferenceSequenceName, r.getPosition.toLong),
      Read(Map[String, Any](
        ("alignedBases" -> r.getAlignedBases),
        ("baseQuality" -> r.getBaseQuality),
        ("cigar" -> r.getCigar),
        ("flags" -> r.getFlags),
        ("id" -> r.getId),
        ("mappingQuality" -> r.getMappingQuality),
        ("matePosition" -> r.getMatePosition),
        ("mateReferenceSequenceName" -> r.getMateReferenceSequenceName),
        ("name" -> r.getName),
        ("originalBases" -> r.getOriginalBases),
        ("position" -> r.getPosition),
        ("readsetId" -> r.getReadsetId),
        ("referenceSequenceName" -> r.getReferenceSequenceName),
        ("tags" -> r.getTags.toMap),
        ("templateLength" -> r.getTemplateLength))))
  }
}
