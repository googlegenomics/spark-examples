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

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpRequest
import com.google.api.client.http.HttpRequestInitializer
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.genomics.Genomics
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.Arrays

object Client {
  final val DevStorageScope = "https://www.googleapis.com/auth/devstorage.read_write"
  final val GenomicsScope = "https://www.googleapis.com/auth/genomics"
  final val EmailScope = "https://www.googleapis.com/auth/userinfo.email"

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
  private val jsonFactory = JacksonFactory.getDefaultInstance()
  private val dataStoreFactory = new FileDataStoreFactory(new java.io.File(System.getProperty("user.home"), ".store/genomics_java_client"))

  def apply(applicationName: String, clientSecretsFile: String): Client = {
    val secrets = GoogleClientSecrets.load(jsonFactory,
      new InputStreamReader(new FileInputStream(new File(clientSecretsFile))))

    val flow = new GoogleAuthorizationCodeFlow.Builder(
      httpTransport, jsonFactory, secrets,
      Arrays.asList(DevStorageScope, GenomicsScope, EmailScope)).setDataStoreFactory(dataStoreFactory).build()

    val credential = new AuthorizationCodeInstalledApp(flow, new GooglePromptReceiver()).authorize("user")

    val service = new Genomics.Builder(httpTransport, jsonFactory, credential)
      .setApplicationName(applicationName)
      .setRootUrl("https://www.googleapis.com/")
      .setHttpRequestInitializer(new HttpRequestInitializer() {
        override def initialize(httpRequest: HttpRequest) = {
          credential.initialize(httpRequest)
          httpRequest.setReadTimeout(60000)
          httpRequest.getHeaders().setUserAgent("spark-examples")
          httpRequest.getHeaders().setAcceptEncoding("gzip")
        }
      }).build()

    new Client(service)
  }
}

class Client(val genomics: Genomics) {}

