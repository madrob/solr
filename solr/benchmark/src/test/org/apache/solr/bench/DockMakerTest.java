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
package org.apache.solr.bench;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.booleans;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.bench.generators.Distribution;
import org.apache.solr.bench.generators.RandomDataHistogram;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockMakerTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testBasicCardinalityAlpha() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("Label");

    Docs docs = docs();

    int cardinality = 2;

    docs.field(
        "AlphaCard3",
        strings().alpha().maxCardinality(cardinality).ofLengthBetween(1, 6).tracked(collector));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("AlphaCard3");
      values.add(field.getValue().toString());
    }

    collector.print();

    assertEquals(values.toString(), cardinality, values.size());
  }

  @Test
  public void testBasicCardinalityUnicode() throws Exception {
    Docs docs = docs();
    int cardinality = 4;
    docs.field(
        "UnicodeCard3",
        strings()
            .basicMultilingualPlaneAlphabet()
            .maxCardinality(cardinality)
            .ofLengthBetween(1, 6));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("UnicodeCard3");
      log.info("field={}", doc);
      values.add(field.getValue().toString());
    }

    assertEquals(values.toString(), cardinality, values.size());
  }

  @Test
  public void testBasicCardinalityInteger() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("Label");

    Docs docs = docs();
    int cardinality = 3;

    docs.field("IntCard2", integers().allWithMaxCardinality(cardinality));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("IntCard2");
      values.add(field.getValue().toString());
    }
    assertEquals(values.toString(), cardinality, values.size());

    collector.print();

    if (log.isInfoEnabled()) {
      log.info(values.toString());
    }
  }

  @Test
  public void testBasicInteger() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("Label");

    Docs docs = docs();

    docs.field(
        "IntCard2",
        integers().between(10, 50).tracked(collector).withDistribution(Distribution.Gaussian));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 300; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("IntCard2");
      values.add(field.getValue().toString());
    }

    collector.print(25);

    if (log.isInfoEnabled()) {
      log.info(values.toString());
    }
  }

  @Test
  public void testBasicIntegerId() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("Label");

    Docs docs = docs();

    docs.field("id", integers().incrementing());

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 300; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("id");
      values.add(field.getValue().toString());
    }

    if (log.isInfoEnabled()) {
      log.info(collector.print());
    }
  }

  @Test
  public void testWordList() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("WordList");

    Docs docs = docs();

    docs.field("wordList", strings().wordList().tracked(collector).multi(4));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 1; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("wordList");
      values.add(field.getValue().toString());
    }

    if (log.isInfoEnabled()) {
      log.info(collector.print());
    }
  }

  @Test
  public void testWordListZipfian() throws Exception {
    RandomDataHistogram.Counts collector = new RandomDataHistogram.Counts("Label");

    Docs docs = docs();

    docs.field(
        "wordList",
        strings().wordList().withDistribution(Distribution.Zipfian).tracked(collector).multi(10));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 1; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("wordList");
      values.add(field.getValue().toString());
    }

    if (log.isInfoEnabled()) {
      log.info(collector.print());
    }
  }

  @Test
  public void testGenDoc() {
    Docs docMaker =
        docs()
            .field("id", integers().incrementing())
            .field(
                "facet_s",
                strings()
                    .basicMultilingualPlaneAlphabet()
                    .maxCardinality(integers().between(5, 16))
                    .ofLengthBetween(1, 128))
            .field(booleans().all());

    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = docMaker.inputDocument();
      if (log.isInfoEnabled()) {
        log.info("doc:\n{}", doc);
      }
    }
  }
}
