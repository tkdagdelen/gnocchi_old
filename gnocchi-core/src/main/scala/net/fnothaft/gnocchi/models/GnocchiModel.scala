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

import org.apache.spark.rdd.RDD

trait GnocchiModel {
  val numSamples: RDD[(String, Int)] //(VariantID, NumSamples)
  val numVariants: Int
  val variances: RDD[(String, Double)] // (VariantID, variance)
  val haplotypeBlockDeltas: Map[String, Double]
  val HBDThreshold: Double // threshold for the haplotype block deltas
  val modelType: String // Additive Logistic, Dominant Linear, etc.
  val hyperparameterVal: Map[String, Double]
  val Description: String
  //  val latestTestResult: GMTestResult
  val variables: String // name of the phenotype and covariates used in the model
  val dates: String // dates of creation and update of each model
  val sampleIds: Array[String] // list of sampleIDs from all samples the model has seen.
  val variantModels: RDD[(String, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
  val qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])] // the variant model and the observations that the model must be trained on

  // filters out all variants that don't pass a certian predicate and returns a GnocchiModel containing only those variants.
  def filter: GnocchiModel

  // given a batch of data, update the GnocchiModel with the data (by updating all the VariantModels).
  // Suggest recompute when haplotypeBlockDelta is bigger than some threshold.
  def update(rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[Array[Double]]]): Unit = {

    //    // convert genotypes and phenotypes into observations
    //    val newData = rdd.keyBy(_.sampleId)
    //      // join together the samples with both genotype and phenotype entry
    //      .join(phenotypes.keyBy(_.sampleId))
    //      .map(kvv => {
    //        // unpack the entry of the joined rdd into id and actual info
    //        val (_, p) = kvv
    //        // unpack the information into genotype state and pheno
    //        val (gs, pheno) = p
    //        // extract referenceAllele and phenotype and pack up with p, then group by key
    //        (gs.variantID, p)
    //      }).groupByKey() // RDD[(variantID,(GenotypeState, Phenotype Object)]
    //
    //    // combine the new sample observations with the old ones.
    //    val joinedqrData = qrVariantModels.map(kv => {
    //      val (varModel, obs) = kv
    //      (varModel.variantID, (varModel, obs))
    //    }).join(newData)
    //      .map(kvv => {
    //        val (varId, (oldData, newData)) = kvv
    //        val (varModel, obs) = oldData
    //        (varModel, (obs.toList ++ newData.toList).toArray)
    //      })
    //
    //    // compute the VariantModels from QR factorization for the qrVariants
    //    val qrAssocRDD = joinedqrData.map(kv => {
    //      val (varModel, obs) = kv
    //      (buildVariantModel(varModel, obs))
    //    })
    //
    //    // group new data with correct VariantModel
    //    val vmAndDataRDD = variantModels.join(newData)
    //
    //    // map an update call to all variants
    //    val updatedVMRdd = vmAndDataRDD.map(kvv => {
    //      val (variantId, (model, data)) = kvv
    //      model.update(data)
    //      model
    //    })
    //
    //    // pair up the QR factorization and incrementalUpdate versions of the selected variantModels
    //    val incrementalVsQrRDD = updatedVMrdd.keyBy(_.variantId).join(qrAssocRDD.keyBy(_.variantID)).groupByKey
    //
    //    // compare results and flag variants for recompute
    //    val variantsToFlag = incrementalVsQrRDD.filter(kvv => {
    //      val (id, (increModel, qrModel)) = kvv
    //      val increValue = increModel.incrementalUpdateValue
    //      val qrValue = qrModel.QRFactorizatinoValue
    //      Math.abs(increValue - qrValue) / qrValue > HBDThreshold
    //    }).map(_._1).collect()
    //
    //    variantModels = updatedVMRdd
  }

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample.
  def predict(rdd: RDD[GenotypeState],
              phenotypes: RDD[Phenotype[Array[Double]]])

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): VariantModel

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample and comparing to actual value
  //  def test(rdd: RDD[GenotypeState],
  //           phenotypes: RDD[Phenotype[Array[Double]]]): GMTestResult

  // save the model
  def save

}