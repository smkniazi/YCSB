/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */

package site.ycsb.db.http.ds;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import site.ycsb.db.http.MyHttpException;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/**
 * MyHttpClientSync.
 */
public class MyHttpClientSync extends MyHttpClient {

  private ThreadLocal<CloseableHttpClient> httpClients = new ThreadLocal<>();
  private boolean useTLS;

  public MyHttpClientSync(boolean useTLS) {
    this.useTLS = useTLS;
  }


  public CloseableHttpClient getHttpClient() {
    if (useTLS){
      return getHttpClientTLS();
    } else {
      return getHttpClientNonSSL();
    }
  }
  private CloseableHttpClient getHttpClientTLS(){
    CloseableHttpClient httpClient;
    httpClient = httpClients.get();
    if (httpClient == null) {
      try {
        TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
        SSLContext sslContext =
            SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
            NoopHostnameVerifier.INSTANCE);

        Registry<ConnectionSocketFactory> socketFactoryRegistry =
            RegistryBuilder.<ConnectionSocketFactory>create()
                .register("https", sslsf)
                .register("http", new PlainConnectionSocketFactory())
                .build();

        BasicHttpClientConnectionManager connectionManager =
            new BasicHttpClientConnectionManager(socketFactoryRegistry);
        httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
            .setConnectionManager(connectionManager).build();
        httpClients.set(httpClient);
      } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        e.printStackTrace();
      }
    }
    return httpClient;
  }
  private CloseableHttpClient getHttpClientNonSSL(){
    CloseableHttpClient httpClient;
    httpClient = httpClients.get();
    if (httpClient == null) {
      httpClient = HttpClients.createDefault();
      httpClients.set(httpClient);
    }
    return httpClient;
  }

  @Override
  public String execute(HttpRequestBase req) throws MyHttpException {
    CloseableHttpResponse resp = null;
    try {
      resp = getHttpClient().execute(req);
      if (resp.getStatusLine().getStatusCode() == 200) {
        String b = EntityUtils.toString(resp.getEntity());
        return b;
      }
    } catch (Exception e) {
      throw new MyHttpException(e);
    } finally {
      if (resp != null) {
        try {
          resp.close();
        } catch (IOException e) {
          throw new MyHttpException(e);
        }
      }
    }
    System.out.println(">more details " + resp.getStatusLine());
    System.out.println(">more details " + resp);
    throw new MyHttpException("Req failed code : " + resp.getStatusLine().getStatusCode());
  }
}

