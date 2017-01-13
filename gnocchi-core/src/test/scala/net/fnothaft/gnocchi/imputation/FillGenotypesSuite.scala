/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.imputation

import net.fnothaft.gnocchi.GnocchiFunSuite
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import scala.collection.JavaConverters._
import org.bdgenomics.adam.rdd.variant.GenotypeRDD

class FillGenotypesSuite extends GnocchiFunSuite {

  test("fill in a single variant context") {
    val v = Variant.newBuilder
      .setContigName("1")
      .setStart(1000L)
      .setEnd(1001L)
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    val vc = VariantContext.buildFromGenotypes(Seq(Genotype.newBuilder
      .setVariant(v)
      .setSampleId("sample1")
      .setAlleles(Seq(GenotypeAllele.REF, GenotypeAllele.ALT).asJava)
      .build()))

    val newGts = FillGenotypes.fillInVC(vc,
      Set("sample1", "sample2", "sample3"),
      Seq(GenotypeAllele.REF))

    assert(newGts.size === 3)
    newGts.foreach(gt => gt.getSampleId match {
      case "sample1" => {
        assert(gt.getAlleles.size === 2)
        assert(gt.getAlleles.get(0) === GenotypeAllele.REF)
        assert(gt.getAlleles.get(1) === GenotypeAllele.ALT)
      }
      case _ => {
        assert(gt.getAlleles.size === 1)
        assert(gt.getAlleles.get(0) === GenotypeAllele.REF)
      }
    })
    assert(newGts.map(_.getSampleId)
      .toSeq
      .sorted
      .toSet === Set("sample1", "sample2", "sample3"))
  }

  sparkTest("fill in with diploid no call") {
    val s1 = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val s2 = ClassLoader.getSystemClassLoader.getResource("small2.vcf").getFile
    val s1rdd = sc.loadGenotypes(s1)
    val s2rdd = sc.loadGenotypes(s2)
    val input = s1rdd.rdd.union(s2rdd.rdd)

    assert(input.count === 4)
    assert(input.map(_.getVariant).distinct.count === 3)

    val mergedInput = GenotypeRDD(input, s1rdd.sequences ++ s2rdd.sequences, s1rdd.samples ++ s2rdd.samples)
    val newGts = FillGenotypes(mergedInput,
      useNoCall = true).rdd.collect
    assert(newGts.length === 6)
    assert(newGts.map(_.getVariant).toSet.size === 3)
    assert(newGts.count(_.getSampleId == "sample1") === 3)
    assert(newGts.count(_.getSampleId == "sample2") === 3)
    assert(newGts.map(_.getAlleles.size).forall(_ == 2))
    assert(newGts.flatMap(_.getAlleles.asScala).count(_ == GenotypeAllele.NO_CALL) === 4)
  }

  sparkTest("fill in with haploid ref call") {
    val s1 = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val s2 = ClassLoader.getSystemClassLoader.getResource("small2.vcf").getFile
    val s1rdd = sc.loadGenotypes(s1)
    val s2rdd = sc.loadGenotypes(s2)
    val input = (s1rdd.rdd ++ s2rdd.rdd).cache

    assert(input.count === 4)
    assert(input.map(_.getVariant).distinct.count === 3)

    val mergedInput = GenotypeRDD(input, s1rdd.sequences ++ s2rdd.sequences, s1rdd.samples ++ s2rdd.samples)
    val newGts = FillGenotypes(mergedInput,
      ploidy = 1).rdd.collect

    assert(newGts.length === 6)
    assert(newGts.map(_.getVariant).toSet.size === 3)
    assert(newGts.count(_.getSampleId == "sample1") === 3)
    assert(newGts.count(_.getSampleId == "sample2") === 3)
    assert(newGts.map(_.getAlleles.size).count(_ == 2) === 4)
    assert(newGts.map(_.getAlleles.size).count(_ == 1) === 2)
    assert(newGts.filter(_.getAlleles.size == 1)
      .flatMap(_.getAlleles.asScala)
      .forall(_ == GenotypeAllele.REF))
  }
}
