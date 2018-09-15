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
import sourcecode._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

private[op] object FeatureMacros {

  /**
   * Transform the feature with a given transformation function and input features
   *
   * @param in            input feature
   * @param f             map function
   * @param operationName name of the operation
   * @return transformed feature
   */
  def map[O <: FeatureType, B <: FeatureType](
    in: FeatureLike[O], f: O => B, operationName: String
  ): FeatureLike[B] = macro FeatureMacrosImpl.map[O, B]

}

/**
 * Feature macros for map operations
 */
private[features] class FeatureMacrosImpl(val c: blackbox.Context) {

  // scalastyle:off

  /**
   * Map operation macro that generates stable class names for lambdas functions
   */
  def map[O <: FeatureType : c.WeakTypeTag, B <: FeatureType : c.WeakTypeTag](
    in: c.Expr[FeatureLike[O]], f: c.Expr[O => B], operationName: c.Expr[String]
  ): c.Expr[FeatureLike[B]] = {
    import c.universe._
    // val line = Line.impl(c)
    // val file = File.impl(c)
    // val thiz = This(c.enclosingClass.symbol.asModule.moduleClass)
    // val thiz = c.Expr[FeatureLike[_]](Select(c.prefix.tree, TermName("this")))
    // val sourceFileName = c.Expr[String](Literal(Constant(c.enclosingUnit.source.path.toString)))
    c.Expr(
      q"""
          new com.salesforce.op.stages.base.unary.UnaryLambdaTransformer(
            operationName = $operationName, transformFn = $f
          ).setInput($in).getOutput()
      """
    )
  }
  // scalastyle:on

}
