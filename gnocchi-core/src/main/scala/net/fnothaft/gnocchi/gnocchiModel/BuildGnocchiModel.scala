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

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, GnocchiModel, Phenotype }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait BuildGnocchiModel {

  def apply[T](rdd: RDD[GenotypeState],
               phenotypes: RDD[Phenotype[T]],
               sc: SparkContext,
               save: Boolean = false): GnocchiModel = {

    // call RegressPhenotypes on the data
    val assocs = fit(rdd, phenotypes)

    // extract the model parameters (including p-value) for each variant and build LogisticGnocchiModel
    val model = extractModel(assocs, sc)

    // save the LogisticGnocchiModel
    if (save) {
      //SaveGnocchiModel(model)
    }

    model
  }

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association]

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel

}