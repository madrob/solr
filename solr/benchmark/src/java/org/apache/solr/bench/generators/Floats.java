/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.bench.generators;

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import org.apache.solr.bench.SolrGenerate;
import org.quicktheories.core.Gen;

final class Floats {

  private static final int POSITIVE_INFINITY_CORRESPONDING_INT = 0x7f800000;
  private static final int NEGATIVE_INFINITY_CORRESPONDING_INT = 0xff800000;
  private static final int NEGATIVE_ZERO_CORRESPONDING_INT = Integer.MIN_VALUE;

  static Gen<Float> fromNegativeInfinityToPositiveInfinity() {
    return fromNegativeInfinityToNegativeZero().mix(fromZeroToPositiveInfinity());
  }

  static Gen<Float> fromNegativeInfinityToNegativeZero() {
    return range(NEGATIVE_ZERO_CORRESPONDING_INT, NEGATIVE_INFINITY_CORRESPONDING_INT);
  }

  static Gen<Float> fromZeroToPositiveInfinity() {
    return range(0, POSITIVE_INFINITY_CORRESPONDING_INT);
  }

  static Gen<Float> fromZeroToOne() {
    return SolrGenerate.range(0, 1 << 24).map(i -> i / (float) (1 << 24));
  }

  static Gen<Float> between(float min, float max) {
    checkArguments(min <= max, "Cannot have the maximum (%s) smaller than the min (%s)", max, min);
    float adjustedMax = max - min;
    return fromZeroToOne().map(f -> (f * adjustedMax) + min);
  }

  private static Gen<Float> range(int startInclusive, int endInclusive) {
    return SolrGenerate.range(startInclusive, endInclusive).map(i -> Float.intBitsToFloat(i));
  }
}
