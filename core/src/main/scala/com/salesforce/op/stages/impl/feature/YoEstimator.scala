package com.salesforce.op.stages.impl.feature

import com.salesforce.op.UID
import com.salesforce.op.features.{Feature, FeatureLike, FeatureSparkTypes, FeatureUID}
import com.salesforce.op.features.types.{Text, _}
import com.salesforce.op.stages.base.binary.{BinaryEstimator, BinaryModel, BinaryTransformer}
import com.salesforce.op.stages.base.unary.{UnaryEstimator, UnaryModel}
import com.salesforce.op.stages.{OpPipelineStage1to2, makeOutputName}
import com.sun.tools.javac.code.TypeTag
import com.twitter.algebird.Operators._
import com.twitter.algebird.Semigroup
import org.apache.spark.ml.param.{BooleanParam, StringArrayParam}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

class YoEstimator(val uid: String = UID[YoEstimator], override val stage1OperationName: String = "text",
  override val stage2OperationName: String = "picklist")(
  implicit val tti: TypeTag[Text],
  val tto1: TypeTag[Text],
  val tto2: TypeTag[OPVector],
  val ttiv: TypeTag[Text#Value],
  val ttov1: TypeTag[Text#Value],
  val ttov2: TypeTag[OPVector#Value]
) extends Estimator[YoModel] with OpPipelineStage1to2[Text, Text, PickList] {
  

  private[op] lazy val stage1 = new YoUnary().setInput(in1.asFeatureLike[Text])

  private[op] lazy val stage2 = new YoBinary().setInput(in1.asFeatureLike[Text], stage1.getOutput())

  implicit val iEncoder: Encoder[Text#Value] = FeatureSparkTypes.featureTypeEncoder[Text]
  val iConvert = FeatureTypeSparkConverter[Text]()


  override def fit(dataset: Dataset[_]): YoModel = {
    val model = stage1.fit(dataset).asInstanceOf[YoUnaryModel]
    val stage2 = new YoBinary().setIsCateorical(model.isCategorical)
      .setInput(in1.asFeatureLike[Text], stage1.getOutput())
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
  override def transformFn: Text => Text = (row: Text) => if (isCategorical) Text.empty else row
}

class YoBinary(uid: String = UID[YoBinary],
  operationName: String = "yo") extends BinaryTransformer[Text, Text, PickList](
  operationName = operationName,
  uid = uid) {
  final val isCategorical = new BooleanParam(
    parent = this, name = "isCategorical", doc = "is Categorical?"
  )

  def setIsCateorical(value: Boolean): this.type = set(isCategorical, value)

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

  override def transform(dataset: Dataset[_]): DataFrame = stage2.transform(stage1.transform(dataset))
}

