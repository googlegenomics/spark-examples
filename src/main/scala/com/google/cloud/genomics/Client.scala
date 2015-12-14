/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics

import java.io.File
import java.io.FileReader
import java.io.StringReader
import java.util.Scanner
import scala.util.{Try, Success, Failure}
import com.google.api.services.genomics.Genomics
import com.google.cloud.genomics.utils.CredentialFactory
import com.google.cloud.genomics.utils.GenomicsFactory
import com.google.cloud.genomics.utils.OfflineAuth

object Authentication {
  def getAccessToken(clientSecretsFile: Option[String],
      applicationName: String = "spark-examples") = {
    if(clientSecretsFile.isDefined) {
      System.out.println("\nThis pipeline will make your user credential available to all"
        + " Spark worker processes.  Your credentials may be visible to others with access to the"
        + " machines on which this pipeline is running.");
      System.out.println("Do you want to continue (Y/n)?");
      val kbd = new Scanner(System.in)
      val decision = kbd.nextLine()
      decision match {
        case "yes" | "Yes" | "YES" | "y" | "Y" => "proceed"
        case _ =>  System.exit(0)
      }
      new OfflineAuth(CredentialFactory.getCredentialFromClientSecrets(clientSecretsFile.get, applicationName))
    } else {
      new OfflineAuth()
    }
  }
}

object Client {

  def apply(auth: OfflineAuth, applicationName: String = "spark-examples"): Client = {
    val factory = GenomicsFactory.builder().build()
    new Client(factory.fromOfflineAuth(auth), factory)
  }
}

class Client(val genomics: Genomics, private val factory: GenomicsFactory) {
  def initializedRequestsCount = factory.initializedRequestsCount()
  def unsuccessfulResponsesCount = factory.unsuccessfulResponsesCount()
  def ioExceptionsCount = factory.ioExceptionsCount()
}

