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
    val pathToGenoFile = ClassLoader.getSystemClassLoader.getResource("binaryValGenos.csv").getFile
    val genoCsv = sc.textFile(pathToGenoFile)
    val genoData = genoCsv.map(line => line.split(",")) //get rows

    val pathToPhenoFile = ClassLoader.getSystemClassLoader.getResource("binaryValPhenos.csv").getFile
    val phenoCsv = sc.textFile(pathToPhenoFile)
    val phenoData = phenoCsv.map(line => line.split(",")) //get rows

    // transform it into the right format
    val genoStates = genoData.map(row => {
      val sampleid: String = row(0)
      val loc: Int = row(1).toInt
      val geno: Int = row(2).toInt
      GenotypeState("A", loc, loc + 1, "A", "A", sampleid, geno, 0)
    })

    val phenos = phenoData.map(row => {
      val sampleid: String = row(0)
      val covars: Array[Double] = row.slice(1, 3).map(_.toDouble)
      val phenos: Array[Double] = Array(row(3).toDouble) ++ covars
      MultipleRegressionDoublePhenotype("pheno", sampleid, phenos).asInstanceOf[Phenotype[Array[Double]]]
    })
    println(genoStates.collect())
    println(phenos.collect())

    val expectedPVals = Map(1000 -> 0.198705131, 2000 -> 0.364599906, 3000 -> 0.912237, 4000 -> 0.697518422)
    val resultRdd = AdditiveLogisticEvaluation(genoStates, phenos, Option(sc), k = 0, n = 3)
    val results = resultRdd.collect()

    // Check that we only receive 3 association objects
    assert(results.length == 3)
    // Check that the association dropped was that of location 3000 (refer to above expected P-values)
    assert(!results.map(_._2.variant.getStart).contains(3000))
    // Check that remaining associations fall within thresholds of expected P-values
    assert(results.map(res => {
      val (_, assoc) = res
      val site = assoc.variant.getStart

      val expected = expectedPVals(site.toInt)
      val actual = assoc.statistics("'P Values' aka Wald Tests").asInstanceOf[DenseVector[Double]](1)

      actual >= expected - 0.00005 && actual <= expected + 0.00005
    }).reduce((i, j) => i && j))
  }
}

