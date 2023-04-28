/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import com.github.sbt.jacoco.report.JacocoReportSettings

val scala211 = "2.11.12"
val scala212 = "2.12.10"

ThisBuild / organization := "za.co.absa"
ThisBuild / name         := "fixed-width"
ThisBuild / scalaVersion := scala212
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

lazy val printSparkVersion = taskKey[Unit]("Print Spark version fixed-width is building against.")

Test / parallelExecution := false

//releaseProcess := Seq[ReleaseStep](
//  checkSnapshotDependencies,
//  inquireVersions,
//  runClean,
//  runTest,
//  setReleaseVersion,
//  commitReleaseVersion,
//  tagRelease,
//  releaseStepCommand("publishSigned"),
//  //  releaseStepCommand("sonatypeBundleRelease"),
//  setNextVersion,
//  commitNextVersion,
//  pushChanges
//)

lazy val fixedWidth = (project in file("."))
  .settings(
    name := "fixed-width",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark $sparkVersion")
      sparkVersion
    },
    libraryDependencies ++= baseDependencies
  )

// release settings
releaseCrossBuild := true
addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"fixed-width Jacoco Report - scala:${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

// exclude example
Test / jacocoExcludes := Seq(
//  "za.co.absa.fixedWidth.ValidationsException*", // class and related objects
//  "za.co.absa.fixedWidth.FixedWidthRelation" // class only
)
