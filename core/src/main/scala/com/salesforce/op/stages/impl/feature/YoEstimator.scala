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

import com.salesforce.op.stages.Direction.{Right => DRight, Left => DLeft}
import com.salesforce.op.UID
import com.salesforce.op.features.types.{Text, _}
import com.salesforce.op.stages.Direction
import com.salesforce.op.stages.base.unary.{UnaryEitherEstimator, UnaryEitherModel}
import com.twitter.algebird.Semigroup
import com.twitter.algebird.Operators._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

class YoEstimator(uid: String = UID[YoEstimator]) extends UnaryEitherEstimator[Text, Text, Integral](
  operationName = "yo", uid = uid) with CleanTextFun {
  private implicit val textStatsSeqEnc: Encoder[TextStats] = ExpressionEncoder[TextStats]()

  private def computeTextStats(text: Text#Value, shouldCleanText: Boolean = true): TextStats = {
    val valueCounts = text match {
      case Some(v) => Map(cleanTextFn(v, shouldCleanText) -> 1)
      case None => Map.empty[String, Int]
    }
    TextStats(valueCounts)
  }

  override def fitFn(dataset: Dataset[Option[String]]): UnaryEitherModel[Text, Text, Integral] = {
    implicit val testStatsSG: Semigroup[TextStats] = TextStats.semiGroup(100)
    val valueStats: Dataset[TextStats] = dataset.map(t => computeTextStats(t))
    val aggregatedStats: TextStats = valueStats.reduce(_ + _)
    val isCategorical = aggregatedStats.valueCounts.size <= 100
    new YoModel(uid, operationName, isCategorical){
      override def branching: Option[Direction] = if(isCategorical) Option(DRight) else Option(DLeft)
    }
  }
}

class YoModel(uid: String, operationName: String, isCategorical: Boolean) extends
  UnaryEitherModel[Text, Text, Integral](uid = uid, operationName = operationName) {
  override def transformFn: Either[Text => Text, Text => Integral] = isCategorical match {
    case true => Right((t: Text) => t.hashCode.toIntegral)
    case false => Left((t: Text) => t)
  }
}

