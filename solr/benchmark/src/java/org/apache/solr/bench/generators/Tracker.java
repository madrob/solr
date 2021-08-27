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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class Tracker {
  private final List<Object> values;
  private final String name;
  private final int count;
  private final double percentage;

  public Tracker(List<Object> values, String name, int count, double percentage) {
    this.values = values;
    this.name = name;
    this.count = count;
    this.percentage = percentage;
  }

  public static String display(Object object) {
    if (object == null) return "null";
    if (object instanceof Class) {
      return ((Class) object).getName();
    }
    if (object instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> collection = (Collection<Object>) object;
      String elements = collection.stream().map(Tracker::display).collect(Collectors.joining(", "));
      return String.format(Locale.ROOT, "[%s]", elements);
    }
    if (object.getClass().isArray()) {
      if (object.getClass().getComponentType().isPrimitive()) {
        return getString(object);
      }
      Object[] array = (Object[]) object;
      String elements =
          Arrays.stream(array).map(Tracker::display).collect(Collectors.joining(", "));
      return String.format(Locale.ROOT, "%s{%s}", object.getClass().getSimpleName(), elements);
    }
    if (String.class.isAssignableFrom(object.getClass())) {
      return String.format(Locale.ROOT, "\"%s\"", object.toString().replace('\u0000', '\ufffd'));
    }
    return object.toString().replace('\u0000', '\ufffd');
  }

  private static String getString(Object obj) {
    if (obj == null) {
      return "null";
    }

    try {
      if (obj.getClass().isArray()) {
        if (obj.getClass().getComponentType().isPrimitive()) {
          if (obj instanceof boolean[]) {
            return Arrays.toString((boolean[]) obj);
          }
          if (obj instanceof char[]) {
            return Arrays.toString((char[]) obj);
          }
          if (obj instanceof short[]) {
            return Arrays.toString((short[]) obj);
          }
          if (obj instanceof byte[]) {
            return Arrays.toString((byte[]) obj);
          }
          if (obj instanceof int[]) {
            return Arrays.toString((int[]) obj);
          }
          if (obj instanceof long[]) {
            return Arrays.toString((long[]) obj);
          }
          if (obj instanceof float[]) {
            return Arrays.toString((float[]) obj);
          }
          if (obj instanceof double[]) {
            return Arrays.toString((double[]) obj);
          }
        }
        return Arrays.deepToString((Object[]) obj);
      }

      return obj.toString();
    } catch (Throwable t) {
      if (t instanceof Error) {
        throw t;
      }

      return obj.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(obj));
    }
  }

  public String name() {
    return name;
  }

  public int count() {
    return count;
  }

  public double percentage() {
    return percentage;
  }

  public List<Object> values() {
    return Collections.unmodifiableList(values);
  }

  public String toString() {
    return String.format(
        Locale.ROOT, "%s (%s, %s%%): %s", name, count, percentage, Tracker.display(values));
  }
}
