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
import static org.apache.solr.bench.generators.SourceDSL.integers;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.SplittableRandom;
import org.apache.solr.bench.SolrGenerate;
import org.apache.solr.bench.SolrRandomnessSource;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.SplittableRandomSource;

public class StringsDSL {

  private static final int BASIC_LATIN_LAST_CODEPOINT = 0x007E;
  private static final int BASIC_LATIN_FIRST_CODEPOINT = 0x0020;
  private static final int ASCII_LAST_CODEPOINT = 0x007F;
  private static final int LARGEST_DEFINED_BMP_CODEPOINT = 65533;

  private static final List<String> words;

  private static final int wordsSize;

  static {
    // english word list via https://github.com/dwyl/english-words

    words = new ArrayList<>(1000);
    InputStream inputStream = StringsDSL.class.getClassLoader().getResourceAsStream("words.txt");
    try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name())) {
      while (scanner.hasNextLine()) {
        words.add(scanner.nextLine());
      }
    }

    wordsSize = words.size();
  }

  public WordListGeneratorBuilder wordList() {
    return new WordListGeneratorBuilder(
        new SolrGen<>(
            new SolrGen<>(String.class) {
              @Override
              public String generate(RandomnessSource in) {
                return words.get(
                    integers()
                        .between(0, wordsSize - 1)
                        .withDistribution(this.getDistribution())
                        .generate(in));
              }
            },
            String.class));
  }

  /**
   * Generates integers as Strings, and shrinks towards "0".
   *
   * @return a Source of type String
   */
  public SolrGen<String> numeric() {
    return new SolrGen<>(numericBetween(Integer.MIN_VALUE, Integer.MAX_VALUE), String.class);
  }

  /**
   * Generates integers within the interval as Strings.
   *
   * @param startInclusive - lower inclusive bound of integer domain
   * @param endInclusive - upper inclusive bound of integer domain
   * @return a Source of type String
   */
  public SolrGen<String> numericBetween(int startInclusive, int endInclusive) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between startInclusive (%s) and endInclusive (%s)",
        startInclusive,
        endInclusive);
    return new SolrGen<>(Strings.boundedNumericStrings(startInclusive, endInclusive), String.class);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from all defined code
   * points
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder allPossible() {
    return betweenCodePoints(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from all defined code
   * points in the Basic Multilingual Plane
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder basicMultilingualPlaneAlphabet() {
    return betweenCodePoints(Character.MIN_CODE_POINT, LARGEST_DEFINED_BMP_CODEPOINT);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from Unicode Basic Latin
   * Alphabet
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder basicLatinAlphabet() {
    return betweenCodePoints(BASIC_LATIN_FIRST_CODEPOINT, BASIC_LATIN_LAST_CODEPOINT);
  }

  public StringGeneratorBuilder alpha() {
    return betweenCodePoints('a', 'z' + 1);
  }

  public StringGeneratorBuilder alphaNumeric() {
    return betweenCodePoints(' ', 'z' + 1);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from Unicode Ascii
   * Alphabet
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder ascii() {
    return betweenCodePoints(Character.MIN_CODE_POINT, ASCII_LAST_CODEPOINT);
  }

  /**
   * Strings with characters between two (inclusive) code points
   *
   * @param minInclusive minimum code point
   * @param maxInclusive max code point
   * @return Builder for strings
   */
  public StringGeneratorBuilder betweenCodePoints(int minInclusive, int maxInclusive) {
    return new StringGeneratorBuilder(minInclusive, maxInclusive);
  }

  public static class WordListGeneratorBuilder {
    private SolrGen<String> strings;

    WordListGeneratorBuilder(SolrGen<String> strings) {
      this.strings = strings;
    }

    public SolrGen<String> ofOne() {
      return strings;
    }

    public SolrGen<String> multi(int count) {
      return multiStringGen(strings, count);
    }

    public WordListGeneratorBuilder tracked(RandomDataHistogram.Counts collector) {
      this.strings = this.strings.tracked(collector);
      return this;
    }

    public WordListGeneratorBuilder withDistribution(Distribution distribution) {
      this.strings.withDistribution(distribution);
      return this;
    }
  }

  public static class StringGeneratorBuilder {

    private final int minCodePoint;
    private final int maxCodePoint;
    private Integer cardinalityStart;
    private SolrGen<Integer> maxCardinality;
    private int multi;

    private StringGeneratorBuilder(int minCodePoint, int maxCodePoint) {
      this.minCodePoint = minCodePoint;
      this.maxCodePoint = maxCodePoint;
    }

    /**
     * Generates Strings of a fixed number of code points.
     *
     * @param codePoints - the fixed number of code points for the String
     * @return a a Source of type String
     */
    public SolrGen<String> ofFixedNumberOfCodePoints(int codePoints) {
      checkArguments(
          codePoints >= 0,
          "The number of codepoints cannot be negative; %s is not an accepted argument",
          codePoints);
      return new SolrGen<>(
          Strings.withCodePoints(minCodePoint, maxCodePoint, SolrGenerate.constant(codePoints)),
          String.class);
    }

    /**
     * Generates Strings of a fixed length.
     *
     * @param fixedLength - the fixed length for the Strings
     * @return a Source of type String
     */
    public SolrGen<String> ofLength(int fixedLength) {
      return ofLengthBetween(fixedLength, fixedLength);
    }

    public StringGeneratorBuilder maxCardinality(int max) {
      maxCardinality = SolrGenerate.constant(max);
      return this;
    }

    public StringGeneratorBuilder maxCardinality(SolrGen<Integer> max) {
      maxCardinality = max;
      return this;
    }

    public StringGeneratorBuilder multi(int count) {
      this.multi = count;
      return this;
    }

    //    StringBuilder sb = new StringBuilder();
    //                    for (int i = 0; i < base; i++) {
    //      sb.append(strings.generate(randomness));
    //      if (i < base - 1) {
    //        sb.append(' ');
    //      }
    //    }
    //                    return sb.toString();

    /**
     * Generates Strings of length bounded between minLength and maxLength inclusively.
     *
     * @param minLength - minimum inclusive length of String
     * @param maxLength - maximum inclusive length of String
     * @return a Source of type String
     */
    public SolrGen<String> ofLengthBetween(int minLength, int maxLength) {
      checkArguments(
          minLength <= maxLength,
          "The minLength (%s) is longer than the maxLength(%s)",
          minLength,
          maxLength);
      checkArguments(
          minLength >= 0,
          "The length of a String cannot be negative; %s is not an accepted argument",
          minLength);
      SolrGen<String> strings =
          Strings.ofBoundedLengthStrings(minCodePoint, maxCodePoint, minLength, maxLength);

      if (maxCardinality != null) {
        SolrGen<String> gen =
            new SolrGen<>(
                new SolrGen<>() {
                  public String generate(SolrRandomnessSource in) {
                    Integer maxCard = maxCardinality.generate(in);

                    if (cardinalityStart == null) {
                      cardinalityStart =
                          SolrGenerate.range(0, Integer.MAX_VALUE - maxCard - 1).generate(in);
                    }

                    long seed =
                        SolrGenerate.range(cardinalityStart, cardinalityStart + maxCard - 1)
                            .generate(in);
                    return strings.generate(
                        (RandomnessSource) new SplittableRandomSource(new SplittableRandom(seed)));
                  }

                  public String generate(RandomnessSource in) {
                    return generate((SolrRandomnessSource) in);
                  }
                },
                String.class);
        if (multi > 1) {
          return multiStringGen(gen, multi);
        }
        return new SolrGen<>(gen, String.class);
      } else {
        if (multi > 1) {
          return multiStringGen(strings, multi);
        }
        return new SolrGen<>(strings, String.class);
      }
    }
  }

  private static SolrGen<String> multiStringGen(SolrGen<String> strings, int multi) {
    return new SolrGen<>(MultiString.class) {
      @Override
      public String generate(SolrRandomnessSource in) {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < multi; i++) {
          sb.append(strings.generate(in));
          if (i < multi - 1) {
            sb.append(' ');
          }
        }
        return sb.toString();
      }
    };
  }
}
