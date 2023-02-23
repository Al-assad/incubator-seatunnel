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

package org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.CONSISTENCY;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.CQL;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.DATA_CENTER;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.IAM_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.IAM_USER;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.KEYSPACE;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.SSL_TRUSTSTORE_PATH;

@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class AmazonKeyspacesConfig implements Serializable {

    private String endpoint;

    private String dataCenter;

    private String iamUser;

    private String iamPassword;

    private String sslTruststorePath;

    private String sslTruststorePassword;

    private String keyspace;

    private String cql;

    private ConsistencyLevel consistencyLevel;

    public static AmazonKeyspacesConfig of(Config config) {
        return new AmazonKeyspacesConfig()
                .setEndpoint(getStringValue(config, ENDPOINT))
                .setDataCenter(getStringValue(config, DATA_CENTER))
                .setIamUser(getStringValue(config, IAM_USER))
                .setIamPassword(getStringValue(config, IAM_PASSWORD))
                .setSslTruststorePath(getStringValue(config, SSL_TRUSTSTORE_PATH))
                .setSslTruststorePassword(getStringValue(config, SSL_TRUSTSTORE_PASSWORD))
                .setKeyspace(getStringValue(config, KEYSPACE))
                .setCql(getStringValue(config, CQL))
                .setConsistencyLevel(
                        DefaultConsistencyLevel.valueOf(getStringValue(config, CONSISTENCY)))
                .resolve();
    }

    private static String getStringValue(Config config, Option<String> option) {
        String key = option.key();
        return config.hasPath(key) ? config.getString(key) : option.defaultValue();
    }

    public AmazonKeyspacesConfig resolve() {
        if ((endpoint == null || endpoint.isEmpty()) && dataCenter != null) {
            setEndpoint(String.format("cassandra.%s.amazonaws.com:9142", dataCenter));
        }
        return this;
    }
}
