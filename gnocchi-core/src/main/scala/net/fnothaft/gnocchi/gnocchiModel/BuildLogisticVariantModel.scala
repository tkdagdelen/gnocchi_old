/**
 * Copyright 2016 Taner Dagdelen
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

package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.association.LogisticSiteRegression
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLinearVariantModel._
import net.fnothaft.gnocchi.models.{ AdditiveLogisticVariantModel, Association, VariantModel }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant
import collection.JavaConverters._

object BuildAdditiveLogisticVariantModel extends BuildVariantModel with LogisticSiteRegression with AdditiveVariant {

  def compute(observations: Array[(Double, Array[Double])],
              locus: ReferenceRegion,
              altAllele: String,
              phenotype: String): Association = {

    val clippedObs = arrayClipOrKeepState(observations)
    val variant = new Variant()
    variant.setContigName(locus.referenceName)
    variant.setStart(locus.start)
    variant.setEnd(locus.end)
    variant.setAlternateAllele(altAllele)
    val emptyArr = List[String]().asJava
    variant.setNames(emptyArr)
    variant.setFiltersFailed(emptyArr)
    val assoc = regressSite(clippedObs, variant, phenotype)
    assoc.statistics = assoc.statistics + ("numSamples" -> observations.length)
    assoc
  }

  def extractVariantModel(assoc: Association): VariantModel = {

    val logRegModel = new AdditiveLogisticVariantModel
    logRegModel.setHaplotypeBlock("assoc.HaploTypeBlock")
      .setHyperParamValues(Map[String, Double]())
      .setIncrementalUpdateValue(0.0)
      .setNumSamples(assoc.statistics("numSamples").asInstanceOf[Int]) // assoc.numSamples
      .setVariance(0.0) // assoc.variance
      .setVariantID("assoc.variantID")
      .setWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
      .setIntercept(assoc.statistics("intercept").asInstanceOf[Double])
    logRegModel
  }

  val regressionName = "Additive Logistic Regression"
}

//object BuildDominantLogisticVariantModel extends BuildVariantModel with LogisticSiteRegression with DominantVariant {
//
//  def compute(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Association = {
//
//    val clippedObs = arrayClipOrKeepState(observations)
//    regressSite(clippedObs, locus, altAllele, phenotype)
//  }
//
//  //  def extractVariantModel(assoc: Association): VariantModel = {
//  //
//  //    // code for extracting the VariantModel from the Association
//  //
//  //  }
//  val regressionName = "Dominant Logistic Regression"
//}

