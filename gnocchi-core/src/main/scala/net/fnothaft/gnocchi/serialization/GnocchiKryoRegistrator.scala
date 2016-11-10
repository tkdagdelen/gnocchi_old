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
package net.fnothaft.gnocchi.serialization

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.serialization.{ AvroSerializer, ADAMKryoRegistrator }
import org.bdgenomics.formats.avro.{ Genotype, Variant }
import scala.collection.mutable.{ WrappedArray }
import net.fnothaft.gnocchi.models.{ Phenotype, Association, Similarity }

class GnocchiKryoRegistrator extends ADAMKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo: Kryo)
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(classOf[org.apache.spark.sql.types.Decimal])
    kryo.register(classOf[org.apache.spark.sql.types.DecimalType])
    kryo.register(classOf[org.apache.spark.sql.types.DataType])
    kryo.register(classOf[org.apache.spark.sql.types.MapType])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.ArrayType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.BooleanType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.BinaryType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.ByteType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.CalendarIntervalType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DataType"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.FloatType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.NullType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.TimestampType$"))

    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Int]])

    kryo.register(classOf[org.bdgenomics.formats.avro.Strand])
    kryo.register(classOf[net.fnothaft.gnocchi.models.MultipleRegressionDoublePhenotype])
    kryo.register(classOf[net.fnothaft.gnocchi.models.GenotypeState])
    kryo.register(classOf[org.bdgenomics.adam.models.ReferenceRegion])

    // java.lang
    kryo.register(classOf[java.lang.Class[_]])

    // java.util
    kryo.register(classOf[java.util.ArrayList[_]])
    kryo.register(classOf[java.util.LinkedHashMap[_, _]])
    kryo.register(classOf[java.util.LinkedHashSet[_]])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[java.util.HashSet[_]])

    // scala.collection.mutable
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    //kryo.register(classOf[Phenotype.ofRef[Array[Double]]])

    kryo.register(classOf[Similarity])
    kryo.register(classOf[Association])
    kryo.register(classOf[Genotype], new AvroSerializer[Genotype]())
    kryo.register(classOf[Variant], new AvroSerializer[Variant]())
  }
}
