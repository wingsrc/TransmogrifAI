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

import java.util.concurrent.ConcurrentHashMap

import com.salesforce.op.stages.LambdaPosition

/**
 * A singleton registry for lambda expressions used in [[LambdaTransformer]] factory
 */
private[op] object LambdaRegistry {

  /**
   * Lambda registry: [[LambdaPosition]] -> Function
   */
  private val registry = new ConcurrentHashMap[String, AnyRef]()

  private def keyOf(p: LambdaPosition) = s"${p.fileName}:L${p.line}C${p.column}"

  /**
   * Register lambda expression at position
   */
  def register[A, B](pos: LambdaPosition, fn: A => B): Unit = doRegister(pos, fn)
  def register[A, B, C](pos: LambdaPosition, fn: (A, B) => C): Unit = doRegister(pos, fn)
  def register[A, B, C, D](pos: LambdaPosition, fn: (A, B, C) => D): Unit = doRegister(pos, fn)
  def register[A, B, C, D, E](pos: LambdaPosition, fn: (A, B, C, D) => E): Unit = doRegister(pos, fn)
  private def doRegister(pos: LambdaPosition, fn: AnyRef): Unit = registry.put(keyOf(pos), fn)

  /**
   * Retrieve lambda function at position
   *
   * @param pos unique lambda function source code position
   * @throws RuntimeException if lambda function is registered as specified position
   */
  def function1[A, B](pos: LambdaPosition): A => B = apply[A => B](pos)
  def function2[A, B, C](pos: LambdaPosition): (A, B) => C = apply[(A, B) => C](pos)
  def function3[A, B, C, D](pos: LambdaPosition): (A, B, C) => D = apply[(A, B, C) => D](pos)
  def function4[A, B, C, D, E](pos: LambdaPosition): (A, B, C, D) => E = apply[(A, B, C, D) => E](pos)

  private def apply[T <: AnyRef](pos: LambdaPosition): T =
    Option(registry.get(keyOf(pos))).map(_.asInstanceOf[T]).getOrElse(
      throw new RuntimeException("Lambda registry have no function registered on position: " +
        s"file ${pos.fileName}, line ${pos.line}, column ${pos.column}"
      )
    )
}
