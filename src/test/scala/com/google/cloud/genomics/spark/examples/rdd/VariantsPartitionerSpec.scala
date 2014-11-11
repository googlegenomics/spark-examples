package com.google.cloud.genomics.spark.examples.rdd

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SplitterSpec extends FlatSpec with Matchers {

   val fixedBases = FixedBasesPerReference(100000)

  "FixedBasesPerReference" should "return one split if the requested split size is less than the number of bases" in {
    val splits = fixedBases.getNumberOfSplits(81020)
    splits should equal(1)
  }

  it should "return enough splits to cover all the bases" in {
    val splits = fixedBases.getNumberOfSplits(181020)
    splits should equal(2)
  }

   val fixedSplits = FixedSplitsPerReference(10)

  "FixedSplitsPerReference" should "return the reference size if the requested number of splits is less than the reference size" in {

    val splits = fixedSplits.getNumberOfSplits(6)
    splits should equal(6)
  }

  it should "return the specified number of splits" in {
    val splits = fixedSplits.getNumberOfSplits(25)
    splits should equal(10)
  }

}