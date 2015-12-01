/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.fetcher

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.conf.Configuration
import org.scalatest.{Assertions, FunSuite}

class JenkinsFetcherSuite extends FunSuite with Assertions {

  /** project summary */
  val SLIDER_PROJECT = "slider-project.json"
  /** a test run with a failed build */
  val SLIDER_738 = "slider-738-run.json"
  /** a test run with a successful build */
  val SLIDER_737 = "slider-737-run.json"

  test("ParseProjectReport") {
    val jenkins = new JenkinsFetcher(new Configuration(false),
      "http://builds.apache.org",
      Seq("Slider"),
      "jenkins.csv",
      ", ",
      1000,
      "org.apache")

    val builds = jenkins.parseProjectReport(jenkins.loadJsonResource(SLIDER_PROJECT))
    assert(Seq(738, 737) === builds)

  }

}
