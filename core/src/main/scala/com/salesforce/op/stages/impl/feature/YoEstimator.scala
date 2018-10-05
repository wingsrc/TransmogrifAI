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
package com.salesforce.op.stages.impl.feature

import com.salesforce.op.UID
import com.salesforce.op.features.{FeatureLike, FeatureUID}
import com.salesforce.op.features.types.{Text, _}
import com.salesforce.op.stages.base.binary.BinaryTransformer
import com.salesforce.op.stages.base.unary.{UnaryEstimator, UnaryModel}
import com.salesforce.op.stages.{OpPipelineStage1to2, makeOutputName}

import scala.reflect.runtime.universe.TypeTag
import com.twitter.algebird.Operators._
import com.twitter.algebird.Semigroup
import org.apache.spark.ml.param.BooleanParam
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

class YoEstimator(val uid: String = UID[YoEstimator], val stage1OperationName: String = "text",
  val stage2OperationName: String = "picklist")(
  implicit val i1ttag: TypeTag[Text],
  val o2ttag: TypeTag[PickList],
  val i1ttiv: TypeTag[Text#Value],
  val o2ttov: TypeTag[PickList#Value]
) extends Estimator[YoModel] with OpPipelineStage1to2[Text, Text, PickList] {


  private[op] lazy val stage1 = new YoUnary().setInput(in1.asFeatureLike[Text])

  private[op] lazy val stage2 = new YoBinary().setInput(in1.asFeatureLike[Text], stage1.getOutput())


  override def fit(dataset: Dataset[_]): YoModel = {
    val model = stage1.fit(dataset).asInstanceOf[YoUnaryModel]
    println(model.isCategorical)
    val stage2 = new YoBinary().setIsCategorical(model.isCategorical)
      .setInput(in1.asFeatureLike[Text], stage1.getOutput())
print(stage2.getCategorical)
    new YoModel(stage1 = model, stage2 = stage2, uid = uid)
  }

  protected[op] def output1FeatureUid: String = FeatureUID[Text](uid)

  final def getOutput1FeatureName: String = makeOutputName(output1FeatureUid, getTransientFeatures())

  override def getOutput(): (FeatureLike[Text], FeatureLike[PickList]) = (stage1.getOutput(), stage2.getOutput())


}


class YoUnary(override val uid: String = UID[YoUnary], override val operationName: String = "text")
  extends UnaryEstimator[Text, Text](
    operationName = operationName,
    uid = uid
  ) with CleanTextFun {
  private implicit val textStatsSeqEnc: Encoder[TextStats] = ExpressionEncoder[TextStats]()

  private def computeTextStats(text: Text#Value, shouldCleanText: Boolean = true): TextStats = {
    val valueCounts = text match {
      case Some(v) => Map(cleanTextFn(v, shouldCleanText) -> 1)
      case None => Map.empty[String, Int]
    }
    TextStats(valueCounts)
  }

  override def fitFn(dataset: Dataset[Text#Value]): UnaryModel[Text, Text] = {
    implicit val testStatsSG: Semigroup[TextStats] = TextStats.semiGroup(100)

    val valueStats: Dataset[TextStats] = dataset.map(t => computeTextStats(t))
    val aggregatedStats: TextStats = valueStats.reduce(_ + _)
    val isCategorical = aggregatedStats.valueCounts.size <= 100
    new YoUnaryModel(isCategorical = isCategorical, operationName = operationName, uid = uid)
  }
}

class YoUnaryModel(val isCategorical: Boolean, uid: String = UID[YoEstimator],
  operationName: String = "yo") extends UnaryModel[Text, Text](operationName = operationName, uid = uid) {
  override def transformFn: Text => Text = (row: Text) => {
    if (isCategorical) Text.empty else row
  }
}

class YoBinary(uid: String = UID[YoBinary],
  operationName: String = "yo") extends BinaryTransformer[Text, Text, PickList](
  operationName = operationName,
  uid = uid) {
  final val isCategorical = new BooleanParam(
    parent = this, name = "isCategorical", doc = "is Categorical?"
  )

  def setIsCategorical(value: Boolean): this.type = set(isCategorical, value)

  def getCategorical: Boolean = $(isCategorical)
  setDefault(isCategorical, false)

  override def transformFn: (Text, Text) => PickList = { (i1: Text, i2: Text) =>
    if ($(isCategorical)) i1.value.toPickList else PickList.empty
  }
}


class YoModel
(val stage1: YoUnaryModel, val stage2: YoBinary, val uid: String)(
  implicit val tti1: TypeTag[Text],
  val tti2: TypeTag[Text],
  val tto: TypeTag[OPVector],
  val ttov: TypeTag[OPVector#Value]
) extends Model[YoModel] with OpPipelineStage1to2[Text, Text, PickList] {
  override def stage1OperationName: String = stage1.operationName

  override def stage2OperationName: String = stage2.operationName

  override def getOutput(): (FeatureLike[Text], FeatureLike[PickList]) = (stage1.getOutput(), stage2.getOutput())

  override def transform(dataset: Dataset[_]): DataFrame = {
    val ds = stage1.transform(dataset)
    println("Stage2")
    println(stage2.getCategorical)
    stage2.transform(ds)
  }
}

