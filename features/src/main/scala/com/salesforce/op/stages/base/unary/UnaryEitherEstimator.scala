/*
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.op.stages.base.unary

import com.salesforce.op.features.FeatureSparkTypes
import com.salesforce.op.features.types.{FeatureType, FeatureTypeSparkConverter}
import com.salesforce.op.stages.{OpPipelineStage1Either}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.util.ClosureUtils

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/**
 * Takes a single input feature and performs a fit operation in order to define a transformation (model)
 * for that feature.
 *
 * @param operationName unique name of the operation this stage performs
 * @param uid           uid for instance
 * @param tti           type tag for input
 * @param tto1          type tag for output
 * @param ttiv          type tag for input value
 * @param ttov1         type tag for output value
 * @param tto2          type tag for output
 * @param ttov2         type tag for output value
 * @tparam I  input feature type
 * @tparam O1 output feature type
 * @tparam O2 output feature type
 */
abstract class UnaryEitherEstimator[I <: FeatureType, O1 <: FeatureType, O2 <: FeatureType]
(
  val operationName: String,
  val uid: String
)(implicit val tti: TypeTag[I],
  val tto1: TypeTag[O1],
  val tto2: TypeTag[O2],
  val ttiv: TypeTag[I#Value],
  val ttov1: TypeTag[O1#Value],
  val ttov2: TypeTag[O2#Value]
) extends Estimator[UnaryEitherModel[I, O1, O2]] with OpPipelineStage1Either[I, O1, O2] {

  // Encoders & converters
  implicit val iEncoder: Encoder[I#Value] = FeatureSparkTypes.featureTypeEncoder[I]
  val iConvert = FeatureTypeSparkConverter[I]()

  /**
   * Function that fits the unary model
   */
  def fitFn(dataset: Dataset[I#Value]): UnaryEitherModel[I, O1, O2]

  /**
   * Check if the stage is serializable
   *
   * @return Failure if not serializable
   */
  final override def checkSerializable: Try[Unit] = ClosureUtils.checkSerializable(fitFn _)

  /**
   * Spark operation on dataset to produce Dataset
   * for constructor fit function and then turn output function into a Model
   *
   * @param dataset input data for this stage
   * @return a fitted model that will perform the transformation specified by the function defined in constructor fit
   */
  override def fit(dataset: Dataset[_]): UnaryEitherModel[I, O1, O2] = {
    setInputSchema(dataset.schema).transformSchema(dataset.schema)

    val df = dataset.select(in1.name)
    val ds = df.map(r => iConvert.fromSpark(r.get(0)).value)
    val model = fitFn(ds)
    val branching = model.branching

    model
      .setParent(this)
      .setInput(in1.asFeatureLike[I])
      .setMetadata(getMetadata())
      .setOutputFeatureName(getOutputFeatureName)
    model.branching = branching
    model
  }

}

/**
 * Extend this class and return it from your [[UnaryEstimator]] fit function.
 * Takes a single input feature and produces a single new output feature using
 * the specified function. Performs row wise transformation specified in transformFn.
 *
 * @param operationName unique name of the operation this stage performs
 * @param uid           uid for instance
 * @param tti           type tag for input
 * @param tto1          type tag for output
 * @param ttov1         type tag for output value
 * @param tto2          type tag for output
 * @param ttov2         type tag for output value
 * @tparam I  input type
 * @tparam O1 output type
 * @tparam O2 output type
 */
abstract class UnaryEitherModel[I <: FeatureType, O1 <: FeatureType, O2 <: FeatureType]
(
  val operationName: String,
  val uid: String
)(
  implicit val tti: TypeTag[I],
  val tto1: TypeTag[O1],
  val ttov1: TypeTag[O1#Value],
  val tto2: TypeTag[O2],
  val ttov2: TypeTag[O2#Value]
) extends Model[UnaryEitherModel[I, O1, O2]] with OpTransformer1Either[I, O1, O2]
