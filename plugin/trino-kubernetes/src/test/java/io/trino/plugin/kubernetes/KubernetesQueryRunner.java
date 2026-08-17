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
package io.trino.plugin.kubernetes;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class KubernetesQueryRunner
{
    static final String KUBERNETES = "kubernetes";

    private KubernetesQueryRunner() {}

    public static Builder builder(TestingKubernetesCluster cluster)
    {
        return new Builder()
                .addConnectorProperty("kubernetes.api-server-uri", cluster.uri().toString())
                .addConnectorProperty("kubernetes.token", cluster.token())
                .addConnectorProperty("kubernetes.insecure-tls", "true")
                .addConnectorProperty("kubernetes.metadata-cache-ttl", "1s");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(KUBERNETES)
                    .setSchema("core")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new KubernetesPlugin());
                queryRunner.createCatalog(KUBERNETES, KUBERNETES, connectorProperties);
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();
        TestingKubernetesCluster cluster = TestingKubernetesCluster.create();
        QueryRunner queryRunner = builder(cluster)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .registerResource(cluster)
                .build();
        Logger log = Logger.get(KubernetesQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
