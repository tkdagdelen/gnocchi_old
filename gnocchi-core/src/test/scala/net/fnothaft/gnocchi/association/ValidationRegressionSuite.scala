/**
 * Copyright 2016 Frank Austin Nothaft, Taner Dagdelen
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

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.GnocchiFunSuite
import org.bdgenomics.adam.models.ReferenceRegion
import net.fnothaft.gnocchi.models.{ Association, GenotypeState, MultipleRegressionDoublePhenotype, Phenotype }
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant

class ValidationRegressionSuite extends GnocchiFunSuite {

  sparkTest("Test pickTopN takes top N associations by p-value") {
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binarySplit.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",")) //get rows

    // transform it into the right format
    val genoStates = data.map(row => {
      val sampleid: String = row(0)
      val loc: Int = row(1).toInt
      val geno: Int = row(2).toInt
      GenotypeState("A", loc, loc + 1, "A", "A", sampleid, geno, 0)
    })

    val phenos = data.map(row => {
      val sampleid: String = row(0)
      val covars: Array[Double] = row.slice(3, 5).map(_.toDouble)
      val phenos: Array[Double] = Array(row(5).toDouble) ++ covars
      MultipleRegressionDoublePhenotype("pheno", sampleid, phenos).asInstanceOf[Phenotype[Array[Double]]]
    })

    val results = AdditiveLogisticEvaluation(genoStates, phenos, k = 1, n = 3).collect()
    assert(results.length == 3)
  }
}

