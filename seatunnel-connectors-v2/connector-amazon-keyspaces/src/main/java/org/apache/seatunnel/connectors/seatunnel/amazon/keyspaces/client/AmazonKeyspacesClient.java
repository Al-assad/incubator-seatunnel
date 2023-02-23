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

package org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.client;

import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesConfig;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.util.Collections;

public class AmazonKeyspacesClient {

    public static CqlSession buildCqlSession(AmazonKeyspacesConfig config) {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withStringList(
                                DefaultDriverOption.CONTACT_POINTS,
                                Collections.singletonList(config.getEndpoint()))
                        .withString(
                                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                                "DefaultLoadBalancingPolicy")
                        .withString(
                                DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER,
                                config.getDataCenter())
                        .withBoolean(
                                DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, false)
                        .withString(
                                DefaultDriverOption.AUTH_PROVIDER_CLASS, "PlainTextAuthProvider")
                        .withString(
                                DefaultDriverOption.AUTH_PROVIDER_USER_NAME, config.getIamUser())
                        .withString(
                                DefaultDriverOption.AUTH_PROVIDER_PASSWORD, config.getIamPassword())
                        .withString(
                                DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
                                "DefaultSslEngineFactory")
                        .withString(
                                DefaultDriverOption.SSL_TRUSTSTORE_PATH,
                                config.getSslTruststorePath())
                        .withString(
                                DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD,
                                config.getSslTruststorePassword())
                        .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false)
                        .build();

        return CqlSession.builder().withConfigLoader(loader).build();
    }
}
