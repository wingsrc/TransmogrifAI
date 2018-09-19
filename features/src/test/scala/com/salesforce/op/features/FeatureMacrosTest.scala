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

package com.salesforce.op.features

import com.salesforce.op.features.types._
import com.salesforce.op.test.TestCommon
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe.TypeTag


@RunWith(classOf[JUnitRunner])
class FeatureMacrosTest extends FlatSpec with TestCommon {

  val feature: FeatureLike[Real] = FeatureBuilder.Real[Double].extract(_.toReal).asPredictor

  Spec(FeatureMacros.getClass) should "provide an unary map function" in {
    val t = FeatureMacros.map[Real, Text](feature, _.value.map(_.toString).toText, "map")
    assertFeature(t)(in = 123.0, out = "123.0".toText, name = t.name, parents = Seq(feature))
  }
  it should "include filename, line and column in the operation name" in {
    val t = FeatureMacros.map[Real, Real](feature, _.value.map(_ + 1.0).toReal, "map")
    t.originStage.operationName shouldBe "map_FeatureMacrosTest_L52C42"
  }
  it should "produce a unique operation name & uid for each map" in {
    val t = FeatureMacros.map[Real, Text](feature, _.value.map(_.toString).toText, "map")
    val tt = FeatureMacros.map[Real, Text](feature, _.value.map(_.toString).toText, "map")
    t.originStage.operationName shouldBe "map_FeatureMacrosTest_L56C42"
    tt.originStage.operationName shouldBe "map_FeatureMacrosTest_L57C43"
    t.originStage.uid should not be tt.originStage.uid
    t.originStage.operationName should not be tt.originStage.operationName
    t should not be tt
  }
  it should "produce a unique operation name & uid for each map with implicit class" in {
    import TestImplicits._
    val t = feature.map[Text](_.value.map(_.toString).toText, "map")
    val tt = feature.map[Text](_.value.map(_.toString).toText, "map")
    t.originStage.operationName shouldBe "map_FeatureMacrosTest_L54C42"
    tt.originStage.operationName shouldBe "map_FeatureMacrosTest_L55C43"
    t.originStage.uid should not be tt.originStage.uid
    t.originStage.operationName should not be tt.originStage.operationName
    t should not be tt
  }
  it should "provide a binary map function" in {
    // TODO
  }
  it should "provide a ternary map function" in {
    // TODO
  }
  it should "provide a quaternary map function" in {
    // TODO
  }

}

object TestImplicits {

  implicit class RichFeature[A <: FeatureType : TypeTag]
  (val feature: FeatureLike[A])(implicit val ftt: TypeTag[A#Value]) {

    final def map[B <: FeatureType : TypeTag](
      f: A => B, operationName: String = "map"
    )(implicit ttv: TypeTag[B#Value]): FeatureLike[B] = FeatureMacros.map[A, B](feature, f, operationName)

  }

}
