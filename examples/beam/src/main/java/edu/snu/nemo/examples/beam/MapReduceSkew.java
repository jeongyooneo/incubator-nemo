/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.compiler.frontend.beam.NemoPipelineOptions;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample MapReduce application for skew experiment.
 */
public final class MapReduceSkew {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceSkew.class.getName());

  /**
   * Private Constructor.
   */
  private MapReduceSkew() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("MapReduceSkew");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
          @Override
          public KV<String, String> apply(final String line) {
            final String[] words = line.split(" +");
            //String key = words[0].substring(1);
            //String value = words[1];
            String key = words[0] + "#" + words[1];
            String value = randomString(50);
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
              @Override
              public String apply(final KV<String, Iterable<String>> kv) {
                final String key = kv.getKey();
                List value = Lists.newArrayList(kv.getValue());
                // val (lower, upper) = s.sortWith((l, r) => l < r).splitAt(s.size / 2)
                Collections.sort(value);
                // LOG.info("key {} value {}", key, value.get(value.size() / 2));
                return key + ", " + value.get(value.size() / 2);
              }
            }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }

  public static String randomString(final int length) {
    Random r = new Random(); // perhaps make it a class variable so you don't make a new one every time
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      char c = (char) (r.nextInt((int) (Character.MAX_VALUE)));
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Assign synthetic skewed keys.
   */
  private static String assignSkewedKey() {
    Random r = new Random();
    int p = r.nextInt(10);
    if (p == 0) {
      return String.join("", Collections.nCopies(20, "192.168"));
    } else if (p == 1) {
      return String.join("", Collections.nCopies(25, "206.51"));
    } else {
      return String.join("", Collections.nCopies(20, "43.252"));
    }
  }

  /**
   * Convert the length of a packet from network trace(tcpdump format) to integer.
   * @param value length of a packet.
   */
  private static Integer assignValue(final String value) {
    Random r = new Random();
    if (StringUtils.isNumeric(value)) {
      Integer num = Integer.parseInt(value);
      if (num > 100) {
        return r.nextInt(10);
      } else {
        return num;
      }
    } else {
      return r.nextInt(10);
    }
  }

  /**
   * Extract key as a network part of an IP address.
   * @param ip IP address of a packet.
   */
  private static String parseIPtoNetwork(final String ip) {
    String network;
    String[] ipparts = ip.split("\\.");
    if (ipparts.length == 1) {
      String[] ipv6 = ip.split(":");
      if (ipv6.length == 1) {
        network = String.join("", Collections.nCopies(15, "192.168.01"));
      } else {
        network = String.join("", Collections.nCopies(15, ipv6[0] + ":" + ipv6[1] + ":" + ipv6[2]));
      }
    } else {
      network = String.join("",
          Collections.nCopies(15, ipparts[0] + "." + ipparts[1] + "." + ipparts[2]));
    }
    return network;
  }
}
