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
package net.fnothaft.gnocchi.models

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait GnocchiModel extends Serializable {
  var numSamples: List[(String, Int)] //(VariantID, NumSamples)
  var numVariants: Int
  var variances: List[(String, Double)] // (VariantID, variance)
  var haplotypeBlockDeltas: Map[String, Double]
  var HBDThreshold: Double // threshold for the haplotype block deltas
  var modelType: String // Additive Logistic, Dominant Linear, etc.
  var hyperparameterVal: Map[String, Double]
  var Description: String
  //  val latestTestResult: GMTestResult
  var variables: String // name of the phenotype and covariates used in the model
  var dates: String // dates of creation and update of each model
  var sampleIds: List[String] // list of sampleIDs from all samples the model has seen.
  var variantModels: List[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
  var qrVariantModels: List[(VariantModel, Array[(Double, Array[Double])])] // the variant model and the observations that the model must be trained on

  // filters out all variants that don't pass a certian predicate and returns a GnocchiModel containing only those variants.
  //  def filter: GnocchiModel

  // given a batch of data, update the GnocchiModel with the data (by updating all the VariantModels).
  // Suggest recompute when haplotypeBlockDelta is bigger than some threshold.
  def update(rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[Array[Double]]],
             sc: SparkContext): Unit = {

    // convert genotypes and phenotypes into observations
    val newData = rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // extract referenceAllele and phenotype and pack up with p, then group by key

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = new Variant()
        variant.setContigName(gs.contigName)
        variant.setStart(gs.start)
        variant.setEnd(gs.end)
        variant.setAlternateAllele(gs.alt)
        val ob = (clipOrKeepState(gs), Array(pheno.value)).asInstanceOf[(Double, Array[Double])]
        (variant, ob)
      }).groupByKey()

    // combine the new sample observations with the old ones for the qr variants.
    val joinedqrData = sc.parallelize(qrVariantModels).map(kv => {
      val (varModel, obs) = kv
      (varModel.variant, (varModel, obs))
    }).join(newData)
      .map(kvv => {
        val (variant, (oldData, newData)) = kvv
        val (varModel, obs) = oldData
        val observs = (obs.toList ::: newData.toList).asInstanceOf[Array[(Double, Array[Double])]]
        (varModel, observs): (VariantModel, Array[(Double, Array[Double])])
      })

    // compute the VariantModels from QR factorization for the qrVariants
    val qrRDD = joinedqrData.map(kv => {
      val (varModel, obs) = kv
      (buildVariantModel(varModel, obs), obs)
    })

    // group new data with correct VariantModel
    val vmAndDataRDD = sc.parallelize(variantModels).join(newData)

    // map an update call to all variants
    val updatedVMRdd = vmAndDataRDD.map(kvv => {
      val (variant, (model, data)) = kvv
      model.update(data.toArray, new ReferenceRegion(variant.getContigName, variant.getStart, variant.getEnd), variant.getAlternateAllele, model.phenotype)
      (variant, model)
    })

    // pair up the QR factorization and incrementalUpdate versions of the selected variantModels
    val qrAssocRDD = qrRDD.map(elem => {
      val (varModel, obs) = elem
      varModel
    })
    val incrementalVsQrRDD = updatedVMRdd.join(qrAssocRDD.keyBy(_.variant))

    // compare results and flag variants for recompute
    val variantsToFlag = incrementalVsQrRDD.filter(kvv => {
      val (variant, (increModel, qrModel)): (Variant, (VariantModel, VariantModel)) = kvv
      val increValue = increModel.incrementalUpdateValue
      val qrValue = qrModel.QRFactorizationValue
      Math.abs(increValue - qrValue) / qrValue > HBDThreshold
    }).map(_._1).collect

    variantModels = updatedVMRdd.collect.toList
    qrVariantModels = qrRDD.collect.toList
  }

  def clipOrKeepState(gs: GenotypeState): Double

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample.
  //  def predict(rdd: RDD[GenotypeState],
  //              phenotypes: RDD[Phenotype[Array[Double]]])

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): VariantModel

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample and comparing to actual value
  //  def test(rdd: RDD[GenotypeState],
  //           phenotypes: RDD[Phenotype[Array[Double]]]): GMTestResult

  // save the model
  //TODO: write save functionality
  //  def save

}

trait Additive extends GnocchiModel {

  def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant extends GnocchiModel {

  def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}