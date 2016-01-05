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

import org.apache.hadoop.conf.Configuration
import org.scalatest.{Assertions, BeforeAndAfter, FunSuite, Matchers}

import org.apache.spark.Logging

class JenkinsFetcherSuite extends FunSuite with Assertions with Matchers with BeforeAndAfter
  with Logging {

  /** project summary */
  val SLIDER_PROJECT = "slider-project.json"
  /** a test run with a failed build */
  val SLIDER_738 = "slider-738-run.json"
  /** a test run with a successful build */
  val SLIDER_737 = "slider-737-run.json"

  val SLIDER_URL = "https://builds.apache.org/job/Slider-develop/"

  var jenkins: JenkinsFetcher = _

  before {
    jenkins = new JenkinsFetcher(new Configuration(false),
      "http://builds.apache.org",
      Seq("Slider"),
      "jenkins.csv",
      ", ",
      1000,
      "org.apache")
  }

  test("ParseProjectReport") {
    jenkins.parseProjectReport(jenkins.loadJsonResource(SLIDER_PROJECT)) should be (Seq(738, 737))
  }

  test("Parse-build-738") {
    val json = jenkins.loadJsonResource(SLIDER_738)
    val buildReport = jenkins.parseBuildReport(SLIDER_URL,"slider", json, 0 )

  }


}
