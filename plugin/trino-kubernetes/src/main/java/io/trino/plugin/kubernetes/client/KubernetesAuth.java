/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kubernetes.client;

import io.trino.spi.TrinoException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import java.net.Socket;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_AUTHENTICATION_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Resolved connection and authentication material for a Kubernetes API server.
 */
public record KubernetesAuth(
        URI serverUri,
        Optional<String> token,
        Optional<List<X509Certificate>> clientCertificateChain,
        Optional<PrivateKey> clientKey,
        Optional<List<X509Certificate>> caCertificates,
        boolean insecureTls)
{
    public KubernetesAuth
    {
        requireNonNull(serverUri, "serverUri is null");
        requireNonNull(token, "token is null");
        clientCertificateChain = clientCertificateChain.map(List::copyOf);
        requireNonNull(clientKey, "clientKey is null");
        caCertificates = caCertificates.map(List::copyOf);
    }

    public SSLContext createSslContext()
    {
        try {
            KeyManager[] keyManagers = null;
            if (clientCertificateChain.isPresent() && clientKey.isPresent()) {
                KeyStore keyStore = KeyStore.getInstance("PKCS12");
                keyStore.load(null, null);
                keyStore.setKeyEntry("client", clientKey.get(), new char[0], clientCertificateChain.get().toArray(new X509Certificate[0]));
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, new char[0]);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            TrustManager[] trustManagers = null;
            if (insecureTls) {
                trustManagers = new TrustManager[] {new InsecureTrustManager()};
            }
            else if (caCertificates.isPresent()) {
                KeyStore trustStore = KeyStore.getInstance("PKCS12");
                trustStore.load(null, null);
                int index = 0;
                for (X509Certificate certificate : caCertificates.get()) {
                    trustStore.setCertificateEntry("ca-" + index, certificate);
                    index++;
                }
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
                trustManagers = trustManagerFactory.getTrustManagers();
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, null);
            return sslContext;
        }
        catch (GeneralSecurityException | java.io.IOException e) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to initialize TLS context for the Kubernetes API server", e);
        }
    }

    private static final class InsecureTrustManager
            extends X509ExtendedTrustManager
    {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }
}
