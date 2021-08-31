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

import java.util.function.Function;
import org.apache.solr.bench.SolrRandomnessSource;
import org.quicktheories.api.AsString;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.BenchmarkRandomSource;

public class SolrGen<T> implements Gen<T> {

  protected final Gen<T> child;
  private final Class<?> type;
  private Distribution distribution = Distribution.Uniform;

  protected long start;
  protected long end;

  protected SolrGen() {
    this(null);
  }

  @SuppressWarnings("unchecked")
  public SolrGen(Gen<T> child, Class<?> type) {
    this.child = child;
    if (child instanceof SolrGen) {
      ((SolrGen<Object>) child).distribution = distribution;
    }
    this.type = type;
  }

  SolrGen(Class<?> type) {
    child = null;
    this.type = type;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T generate(RandomnessSource in) {
    // System.out.println("gen : " + toString() + " child: " + child.toString());

    if (child == null && start == end) {
      return generate((SolrRandomnessSource) in);
    }

    if (child == null) {
      return (T) Integer.valueOf((int) ((SolrRandomnessSource) in).next(start, end));
    }

    if (in instanceof BenchmarkRandomSource) {
      return child.generate(((BenchmarkRandomSource) in).withDistribution(distribution));
    }
    return child.generate(in);
  }

  @SuppressWarnings("unchecked")
  public T generate(SolrRandomnessSource in) {
    if (child == null && start == end) {
      return generate((RandomnessSource) in);
    }

    if (child == null) {
      return (T) Integer.valueOf((int) in.next(start, end));
    }

    if (child instanceof SolrGen) {
      return (T) ((SolrGen<Object>) child).generate(in.withDistribution(distribution));
    }

    return child.generate((RandomnessSource) in);
  }

  @SuppressWarnings("unchecked")
  public Class type() {
    SolrGen other = this;
    while (true) {
      if (other.type == null && other.child instanceof SolrGen) {
        other = ((SolrGen<Object>) other.child);
        continue;
      }
      return other.type;
    }
  }

  public SolrGen<T> describedAs(AsString<T> asString) {
    return new SolrDescribingGenerator<>(this, asString);
  }

  public Gen<T> mix(Gen<T> rhs, int weight) {
    return new SolrGen<T>() {
      @Override
      public T generate(SolrRandomnessSource in) {
        while (true) {
          long picked = in.next(0, 99);
          if (picked >= weight) {
            continue;
          }
          return ((SolrGen<T>) rhs).generate(in);
        }
      }
    };
  }

  /**
   * Flat maps generated values with supplied function
   *
   * @param mapper function to map with
   * @param <R> Type to map to
   * @return A Gen of R
   */
  public <R> Gen<R> flatMap(Function<? super T, Gen<? extends R>> mapper) {
    return new SolrGen<R>(in -> mapper.apply(generate(in)).generate(in), null);
  }

  public <R> Gen<R> map(Function<? super T, ? extends R> mapper) {
    return new SolrGen<R>(in -> mapper.apply(generate(in)), null);
  }

  public SolrGen<T> tracked(RandomDataHistogram.Counts collector) {
    return new TrackingGenerator<>(this, collector);
  }

  @SuppressWarnings("unchecked")
  public SolrGen<T> withDistribution(Distribution distribution) {
    // System.out.println("set dist gen : " + toString() + " child: " + child.toString());
    this.distribution = distribution;
    if (this.child instanceof SolrGen) {
      ((SolrGen<Object>) this.child).distribution = distribution;
    }
    return this;
  }

  @Override
  public String toString() {
    return "SolrGen{"
        + "child="
        + child
        + ", type="
        + type
        + ", distribution="
        + distribution
        + '}';
  }

  protected Distribution getDistribution() {
    return this.distribution;
  }

  public class TrackingGenerator<G> extends SolrGen<G> {
    private final SolrGen<G> gen;
    private final RandomDataHistogram.Counts collector;

    public TrackingGenerator(SolrGen<G> gen, RandomDataHistogram.Counts collector) {
      this.gen = gen;
      this.collector = collector;
    }

    @Override
    public G generate(RandomnessSource in) {
      G val;
      if (in instanceof BenchmarkRandomSource) {
        val =
            gen.generate(
                (RandomnessSource) ((BenchmarkRandomSource) in).withDistribution(distribution));
        collector.collect(val);
        return val;
      }
      val = gen.generate(in);
      collector.collect(val);
      return val;
    }

    public SolrGen<G> withDistribution(Distribution distribution) {
      if (this.child != null) {
        throw new IllegalStateException();
      }
      gen.withDistribution(distribution);

      return this;
    }

    @Override
    public String toString() {
      return "TrackingSolrGen{"
          + "wrapped="
          + gen
          + ", type="
          + type
          + ", distribution="
          + distribution
          + '}';
    }
  }
}

class SolrDescribingGenerator<G> extends SolrGen<G> {

  private final Gen<G> child;
  private final AsString<G> toString;

  public SolrDescribingGenerator(Gen<G> child, AsString<G> toString) {
    this.child = child;
    this.toString = toString;
  }

  @Override
  public G generate(RandomnessSource in) {
    G val;
    val = child.generate(in);
    return val;
  }

  @Override
  public G generate(SolrRandomnessSource in) {
    G val;
    val = child.generate((RandomnessSource) in);
    return val;
  }

  public String asString(G t) {
    return toString.asString(t);
  }
}
