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

import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.quicktheories.api.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomDataHistogram {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final char VERTICAL_BAR = '│';
  public static final String COUNT = "Count";
  public static final int MAX_WIDTH = 120;
  public static final String LABEL = "Label";
  public static final char HISTO_DOT = '⦿';
  private final int bucketCnt;

  public RandomDataHistogram(int bucketCnt) {
    this.bucketCnt = bucketCnt;
  }

  public String print(List<Tracker> entries, String label) {
    return print(entries, false, label);
  }

  public String print(List<Tracker> entries, boolean countsComparator, String label) {
    entries.sort(
        countsComparator
            ? (left, right) -> {
              try {
                Integer leftFirst = left.count();
                Integer rightFirst = right.count();
                return rightFirst.compareTo(leftFirst);
              } catch (ClassCastException castException) {
                return -Integer.compare(left.count(), right.count());
              }
            }
            : comparator());
    if (bucketCnt > -1) {
      return print(label, bucket(bucketCnt, entries), countsComparator);
    } else {
      List<Bucket> buckets = new ArrayList<>(32);
      for (Tracker entry : entries) {
        Bucket bucket = new Bucket(label(entry), entry.count());
        buckets.add(bucket);
      }
      return print(label, buckets, countsComparator);
    }
  }

  @SuppressWarnings("unchecked")
  private Comparator<? super Tracker> comparator() {
    if (bucketCnt > -1) {
      return (left, right) -> 0;
    }
    return (left, right) -> {
      try {
        Comparable<Object> leftFirst = (Comparable<Object>) left.values().get(0);
        Comparable<Object> rightFirst = (Comparable<Object>) right.values().get(0);
        return leftFirst.compareTo(rightFirst);
      } catch (ClassCastException castException) {
        return -Integer.compare(left.count(), right.count());
      }
    };
  }

  private String label(final Tracker tracker) {
    if (bucketCnt > -1) {
      return "<--->";
    }
    return tracker.name();
  }

  protected static List<Bucket> bucket(int bucketCnt, final List<Tracker> entries) {
    Pair<BigInteger, BigInteger> minMax = minMax(entries);
    BigInteger min = minMax._1;
    BigInteger max = minMax._2;

    List<Pair<BigInteger, Bucket>> topsAndBuckets = bucketsAndMax(bucketCnt, min, max);

    for (Tracker entry : entries) {
      Bucket bucket = bucket(topsAndBuckets, new BigDecimal(entry.values().get(0).toString()));
      bucket.addCount(entry.count());
    }

    return topsAndBuckets.stream()
        .map(bigIntegerBucketPair -> bigIntegerBucketPair._2)
        .collect(Collectors.toList());
  }

  private String print(String label, final List<Bucket> buckets, boolean countComparator) {
    int labelWidth =
        Math.max(
            LABEL.length(),
            buckets.stream().mapToInt(bucket2 -> bucket2.label.length()).max().orElse(0));
    int maxCnt = buckets.stream().mapToInt(bucket1 -> bucket1.count).max().orElse(0);
    int cntWidth = Math.max(COUNT.length(), (int) Math.max(1, Math.floor(Math.log10(maxCnt)) + 1));

    StringBuilder report = new StringBuilder(32);
    String header =
        "%1$4s "
            + VERTICAL_BAR
            + " %2$"
            + labelWidth
            + "s "
            + VERTICAL_BAR
            + " %3$"
            + cntWidth
            + "s "
            + VERTICAL_BAR
            + " %4$s";
    String bucket =
        "%1$4s "
            + VERTICAL_BAR
            + " %2$"
            + labelWidth
            + "s "
            + VERTICAL_BAR
            + " %3$"
            + cntWidth
            + "d "
            + VERTICAL_BAR
            + " %4$s";

    report
        .append(this.getClass().getSimpleName())
        .append("   (")
        .append(countComparator ? COUNT : label)
        .append(" Sort)\n\n");
    report.append(String.format(Locale.ROOT, header, "", label, COUNT, "")).append('\n');
    String barRuler = String.valueOf('―').repeat(Math.max(0, maxCnt + 10));
    report
        .append(String.format(Locale.ROOT, header, "", "", "", barRuler).replace(" ", "―"))
        .append('\n');

    int size = buckets.size();
    for (int i = 0; i < size; i++) {
      report
          .append(
              String.format(
                  Locale.ROOT,
                  bucket,
                  i,
                  buckets.get(i).label,
                  buckets.get(i).count,
                  String.valueOf(HISTO_DOT)
                      .repeat(
                          Math.max(
                              0,
                              (int)
                                  (buckets.get(i).count
                                      / Math.max(1.0, maxCnt / (double) MAX_WIDTH))))))
          .append('\n');
    }

    return report.toString();
  }

  private static RandomDataHistogram.Bucket bucket(
      List<Pair<BigInteger, RandomDataHistogram.Bucket>> bucketsAndMax, BigDecimal value) {
    int size = bucketsAndMax.size();
    for (int i = 0; i < size; i++) {
      Pair<BigInteger, RandomDataHistogram.Bucket> bucketAndMax = bucketsAndMax.get(i);
      BigInteger top = bucketAndMax._1;
      if (value.compareTo(new BigDecimal(top)) < 0) {
        return bucketAndMax._2;
      }
      if (i == size - 1) {
        return bucketAndMax._2;
      }
    }
    throw new IllegalArgumentException("No bucket found for value: " + value);
  }

  private static List<Pair<BigInteger, RandomDataHistogram.Bucket>> bucketsAndMax(
      int bucketCnt, final BigInteger min, final BigInteger max) {
    BigInteger range = max.subtract(min);
    BigInteger numberOfBuckets = BigInteger.valueOf(bucketCnt);
    BigInteger step = range.divide(numberOfBuckets);
    BigInteger remainder = range.remainder(numberOfBuckets);
    if (remainder.compareTo(BigInteger.ZERO) != 0) {
      step = step.add(BigInteger.ONE);
    }

    List<Pair<BigInteger, RandomDataHistogram.Bucket>> bucketsAndMax = new ArrayList<>(32);
    BigInteger left = min;
    for (BigInteger index = min.add(step); index.compareTo(max) < 0; index = index.add(step)) {
      String label = String.format(Locale.ROOT, "[%s..%s", left, index) + '[';
      bucketsAndMax.add(Pair.of(index, new RandomDataHistogram.Bucket(label)));
      left = index;
    }
    String label = String.format(Locale.ROOT, "[%s..%s", left, max) + ']';
    bucketsAndMax.add(Pair.of(max, new RandomDataHistogram.Bucket(label)));
    return bucketsAndMax;
  }

  private static Pair<BigInteger, BigInteger> minMax(final List<Tracker> entries) {
    BigDecimal min = null;
    BigDecimal max = null;
    for (Tracker entry : entries) {
      try {
        BigDecimal value = new BigDecimal(entry.values().get(0).toString());
        if (min == null || value.compareTo(min) < 0) {
          min = value;
        }
        if (max == null || value.compareTo(max) > 0) {
          max = value;
        }
      } catch (NumberFormatException e) {
        throw new ParseNumberException("Could not parse " + entry.values().get(0), e);
      }
    }
    if (max == null) max = new BigDecimal(0);
    if (min == null) min = new BigDecimal(0);
    BigInteger maxBigInteger = max.setScale(0, RoundingMode.UP).toBigInteger();
    return Pair.of(min.toBigInteger(), maxBigInteger);
  }

  public static class Bucket {
    private final String label;
    private int count;

    public Bucket(String label) {
      this(label, 0);
    }

    public Bucket(String label, final int count) {
      Objects.requireNonNull(label);
      this.label = label;
      this.count = count;
    }

    void addCount(int count) {
      this.count += count;
    }
  }

  public static class Counts {

    private final Map<List<Object>, Integer> objectToCount = new HashMap<>(32);
    private final String label;
    private List<Tracker> entries = null;

    public Counts(String label) {
      this.label = label;
    }

    public Counts collect(Object... values) {
      if (values.length == 0) {
        throw new IllegalArgumentException("values length must be greater than 0: " + label);
      }
      List<Object> key;
      key = Arrays.asList(values);

      if (!objectToCount.isEmpty()
          && objectToCount.keySet().iterator().next().size() != key.size()) {
        String message = "Value count must be consistent: " + label;
        throw new IllegalArgumentException(message);
      }
      int count = objectToCount.computeIfAbsent(key, any -> 0);
      objectToCount.put(key, ++count);
      entries = null;
      return this;
    }

    public String print() {
      return print(false, -1);
    }

    public String print(int bucketed) {
      return print(false, bucketed);
    }

    public String print(boolean sortByCounts, int bucketed) {
      if (entries == null) {
        int sum = objectToCount.values().stream().mapToInt(aCount -> aCount).sum();
        entries =
            objectToCount.entrySet().stream()
                .sorted(
                    (e1, e2) -> {
                      List<Object> k1 = e1.getKey();
                      List<Object> k2 = e2.getKey();
                      if (k1.size() != k2.size()) {
                        return Integer.compare(k1.size(), k2.size());
                      }
                      return e2.getValue().compareTo(e1.getValue());
                    })
                .filter(entry -> !entry.getKey().equals(Collections.emptyList()))
                .map(
                    entry -> {
                      double percentage = entry.getValue() * 100.0 / sum;
                      return new Tracker(
                          entry.getKey(),
                          entry.getKey().stream()
                              .map(Objects::toString)
                              .collect(Collectors.joining(" ")),
                          entry.getValue(),
                          percentage);
                    })
                .collect(Collectors.toList());
      }

      RandomDataHistogram randomDataHistogram = new RandomDataHistogram(bucketed);
      return randomDataHistogram.print(entries, sortByCounts, label);
    }
  }

  private static class ParseNumberException extends RuntimeException {
    public ParseNumberException(String s, NumberFormatException e) {
      super(s, e);
    }
  }
}
