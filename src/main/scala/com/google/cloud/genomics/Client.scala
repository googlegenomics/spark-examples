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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.services.genomics.Genomics
import com.google.cloud.genomics.utils.GenomicsFactory
import com.google.common.base.Suppliers
import scala.util.{Try, Success, Failure}

object Authentication {
  def getAccessToken(applicationName: String, clientSecretsFile: String) = {
    val verificationCodeReceiver = Suppliers.ofInstance(new GooglePromptReceiver())
    val factory = GenomicsFactory.builder(applicationName)
      .setVerificationCodeReceiver(verificationCodeReceiver).build()
    factory.makeCredential(new File(clientSecretsFile)).getAccessToken()
  }
}

object Client {
  def apply(applicationName: String, accessToken: String): Client = {
    // An IOException can occur when multiple workers on the same machine try to
    // create the directory to hold the stored credentials.
    val factory = Try(GenomicsFactory.builder(applicationName)
      .setReadTimeout(60000).build()) match {
         case Success(f) => f
         case Failure(ex) => {
           // Try one more time.
           GenomicsFactory.builder(applicationName)
             .setReadTimeout(60000).build()
          }
      }
    val service = factory.fromCredential(
        new GoogleCredential().setAccessToken(accessToken))
    new Client(service)
  }
}

class Client(val genomics: Genomics)

