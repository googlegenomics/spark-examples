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
import scala.util.{Try, Success, Failure}
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.genomics.Genomics
import com.google.cloud.genomics.utils.GenomicsFactory
import com.google.common.base.Suppliers
import com.google.cloud.genomics.utils.GenomicsFactory.OfflineAuth

object Authentication {
  def getAccessToken(clientSecretsFile: String,
      applicationName: String = "spark-examples") = {
    val verificationCodeReceiver = Suppliers.ofInstance(new GooglePromptReceiver())
    GenomicsFactory.builder(applicationName)
      .setVerificationCodeReceiver(verificationCodeReceiver).build()
      .getOfflineAuthFromClientSecretsFile(clientSecretsFile)
  }
}

object Client {

  def apply(auth: OfflineAuth, applicationName: String = "spark-examples"): Client = {
    val factory = auth.getDefaultFactory()
    new Client(auth.getGenomics(factory), factory)
  }
}

class Client(val genomics: Genomics, private val factory: GenomicsFactory) {
  def initializedRequestsCount = factory.initializedRequestsCount()
  def unsuccessfulResponsesCount = factory.unsuccessfulResponsesCount()
  def ioExceptionsCount = factory.ioExceptionsCount()
}

