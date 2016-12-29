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
package net.fnothaft.gnocchi.association

import breeze.numerics.log10
import breeze.linalg._
import breeze.numerics._
import net.fnothaft.gnocchi.models.{Association, GenotypeState, Phenotype}
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SQLContext
import org.apache.commons.math3.special.Gamma
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{Contig, Variant}

trait LogisticValidationRegression extends ValidationRegression with LogisticSiteRegression {

  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  def apply[T](rdd: RDD[GenotypeState],
               phenotypes: RDD[Phenotype[T]],
               scOption: Option[SparkContext] = None,
               k: Int = 1,
               n: Int = 1,
               sc: SparkContext,
               monte: Boolean = false,
               threshold: Double = 0.5): Array[RDD[(Array[(String, (Double, Double))], Association)]] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val crossValResults = new Array[RDD[(Array[(String, (Double, Double))], Association)]](k)

    // 1 random 90/10 split
    println("\n\n\n\n\n\n\n k=1, n=1 \n\n\n\n\n\n\n")
    val rdds = genoPhenoRdd.randomSplit(Array(.9, .1))
    val trainRdd = rdds(0)
    val testRdd = rdds(1)

    val regressionResults = applyRegression(trainRdd, testRdd, phenotypes)

    crossValResults(0) = regressionResults.map(site => {
      val (key, value) = site
      val (sampleObservations, association) = value
      val (variant, phenotype) = key

      (predictSiteWithThreshold(sampleObservations.map(p => {
        // unpack p
        val (sampleid, (genotypeState, phenotype)) = p
        // return genotype and phenotype in the correct form
        (clipOrKeepState(genotypeState), phenotype.toDouble, sampleid)
      }).toArray, association, threshold), association)
    })
    crossValResults
  }

  /**
   * This method will predict the phenotype given a certain site, given the association results
   *
   * @param sampleObservations An array containing tuples in which the first element is the coded genotype.
   *                           The second is an Array[Double] representing the phenotypes, where the first
   *                           element in the array is the phenotype to regress and the rest are to be treated as
   *                           covariates. The third is the sampleid.
   * @param association  An Association object that specifies the model trained for this locus
   * @return An array of results with the model applied to the observations
   */

  def predictSite(sampleObservations: Array[(Double, Array[Double], String)],
                  association: Association): Array[(String, (Double, Double))] = {
    predictSiteWithThreshold(sampleObservations, association, 0.5)
  }

  def predictSiteWithThreshold(sampleObservations: Array[(Double, Array[Double], String)],
                  association: Association, threshold: Double): Array[(String, (Double, Double))] = {
    // transform the data in to design matrix and y matrix compatible with mllib's logistic regresion
    val observationLength = sampleObservations(0)._2.length
    val numObservations = sampleObservations.length
    val lp = new Array[LabeledPoint](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var features = new Array[Double](observationLength)
    val samples = new Array[String](sampleObservations.length)
    for (i <- sampleObservations.indices) {
      // rearrange variables into label and features
      features = new Array[Double](observationLength)
      features(0) = sampleObservations(i)._1.toDouble
      sampleObservations(i)._2.slice(1, observationLength).copyToArray(features, 1)
      val label = sampleObservations(i)._2(0)

      // pack up info into LabeledPoint object
      lp(i) = new LabeledPoint(label, new org.apache.spark.mllib.linalg.DenseVector(features))

      samples(i) = sampleObservations(i)._3
    }

    val statistics = association.statistics
    val b = statistics("weights").asInstanceOf[Array[Double]]

    // TODO: Check that this actually matches the samples with the right results.
    // receive 0/1 results from datapoints and model
    val results = predict(lp, b, threshold)
    samples zip results
  }

  def predict(lpArray: Array[LabeledPoint], b: Array[Double], threshold: Double): Array[(Double, Double)] = {
    val expitResults = expit(lpArray, b)
    // (Predicted, Actual)
    val predictions = new Array[(Double, Double)](expitResults.length)
    for (j <- predictions.indices) {
      val res = expitResults(j)
      predictions(j) = (lpArray(j).label, if (expitResults(j) >= threshold) 1.0 else 0.0)
    }
    predictions
  }

  def expit(lpArray: Array[LabeledPoint], b: Array[Double]): Array[Double] = {
    val expitResults = new Array[Double](lpArray.length)
    val bDense = DenseVector(b)
    for (j <- expitResults.indices) {
      val lp = lpArray(j)
      expitResults(j) = 1 / (1 + Math.exp(-DenseVector(1.0 +: lp.features.toArray) dot bDense))
    }
    expitResults
  }
}

object AdditiveLogisticEvaluation extends LogisticValidationRegression with Additive {
  val regressionName = "additiveLogisticEvaluation"
}

object DominantLogisticEvaluation extends LogisticValidationRegression with Dominant {
  val regressionName = "dominantLogisticEvaluation"
}

object AdditiveLogisticMonteCarloEvaluation extends LogisticValidationRegression with Additive with MonteCarlo {
  val regressionName = "additiveLogisticEvaluation"
}

object AdditiveLogisticKfoldsEvaluation extends LogisticValidationRegression with Additive with kfolds {
  val regressionName = "additiveLogisticEvaluation"
}

object AdditiveLogisticProgressiveEvaluation extends LogisticValidationRegression with Additive with Progressive {
  val regressionName = "additiveLogisticEvaluation"
}
