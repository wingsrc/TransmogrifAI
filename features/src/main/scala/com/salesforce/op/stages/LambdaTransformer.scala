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

package com.salesforce.op.stages

import com.salesforce.op.UID
import com.salesforce.op.features.LambdaRegistry
import com.salesforce.op.features.types.FeatureType
import com.salesforce.op.stages.base.unary.{UnaryLambdaTransformer, UnaryTransformer}

import scala.reflect.runtime.universe.TypeTag

/**
 * Lambda transformers factory - one place to create all lambda transformers:
 * unary, binary, ternary, quaternary, sequence etc.
 */
case object LambdaTransformer {

  /**
   * Transformer that takes a single input feature and produces
   * a single new output feature using the specified function.
   * Performs row wise transformation specified in transformFn.
   */
  def unary[A <: FeatureType : TypeTag, B <: FeatureType : TypeTag](
    fn: A => B,
    operationName: String,
    uid: String = UID[UnaryLambdaTransformer[A, B]]
  )(implicit ttov: TypeTag[B#Value], pos: sourcecode.Position): UnaryTransformer[A, B] = {
    val lambdaPosition = new LambdaPosition(pos)
    LambdaRegistry.register[A, B](lambdaPosition, fn)
    new UnaryLambdaTransformer[A, B](
      position = lambdaPosition,
      operationName = operationNameWithPosition(operationName, lambdaPosition),
      uid = uid
    )
  }

  // TODO: add factories for binary, ternary, quaternary, sequence etc.

  private def operationNameWithPosition(operationName: String, pos: LambdaPosition): String = {
    s"${operationName}_${pos.fileName}_L${pos.line}C${pos.column}"
  }
}

/**
 * Unique source code position for lambda function
 */
case class LambdaPosition(fileName: String, line: Int, column: Int) {
  def this(pos: sourcecode.Position) = this(pos.file.split('/').last.split('.').head, pos.line, pos.column)
}
