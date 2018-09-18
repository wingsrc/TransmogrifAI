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

/**
 * A singleton registry for lambda expressions used in feature `.map` operations
 */
private[op] object LambdaRegistry {

  /**
   * Lambda unique source code position
   */
  case class Position(fileName: String, line: Int, column: Int)

  private val registry = new java.util.concurrent.ConcurrentHashMap[String, AnyRef]()

  /**
   * Register lambda function at specified position
   */
  def apply(fileName: String, line: Int, column: Int, fn: AnyRef): Position = {
    val position = Position(fileName, line, column)
    register(position, fn)
    position
  }
  def register(pos: Position, fn: AnyRef): Unit = registry.put(pos.toString, fn)

  /**
   * Retrieve lambda function by position
   */
  def function1[A, B](pos: Position): A => B = apply[A => B](pos)
  def function2[A, B, C](pos: Position): (A, B) => C = apply[(A, B) => C](pos)
  def function3[A, B, C, D](pos: Position): (A, B, C) => D = apply[(A, B, C) => D](pos)
  def function4[A, B, C, D, E](pos: Position): (A, B, C, D) => E = apply[(A, B, C, D) => E](pos)

  private def apply[T <: AnyRef](pos: Position): T =
    Option(registry.get(pos.toString)).map(_.asInstanceOf[T]).getOrElse(
      throw new RuntimeException(s"Lambda registry have no function registered " +
        s"at ${pos.fileName} line ${pos.line}, column ${pos.column}"
      )
    )
}
