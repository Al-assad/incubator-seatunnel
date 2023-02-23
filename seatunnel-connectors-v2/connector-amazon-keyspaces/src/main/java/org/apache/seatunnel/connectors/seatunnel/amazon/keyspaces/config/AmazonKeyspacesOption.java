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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import java.util.Arrays;
import java.util.List;

public class AmazonKeyspacesOption {

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Amazon Keyspaces endpoint, such as \"cassandra.us-east-2.amazonaws.com:9142\","
                                    + "When the value is null, the endpoint represented by the datacenter "
                                    + "will be used by default");

    public static final Option<String> DATA_CENTER =
            Options.key("datacenter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon Keyspaces data center, such as \"us-east-2\"");

    public static final Option<String> IAM_USER =
            Options.key("iam_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon Keyspaces IAM service username");

    public static final Option<String> IAM_PASSWORD =
            Options.key("iam_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon Keyspaces IAM service password");

    public static final Option<String> SSL_TRUSTSTORE_PATH =
            Options.key("ssl_truststore_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon Keyspaces SSL truststore file path");

    public static final Option<String> SSL_TRUSTSTORE_PASSWORD =
            Options.key("ssl_truststore_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon Keyspaces SSL truststore password");

    public static final Option<String> KEYSPACE =
            Options.key("keyspace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("keyspace name of Amazon keyspaces");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table name of Amazon keyspaces");

    public static final Option<String> CQL =
            Options.key("cql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("data query statement");

    public static final Option<String> CONSISTENCY =
            Options.key("consistency")
                    .stringType()
                    .defaultValue("LOCAL_QUORUM")
                    .withDescription("consistency level of read or write operations");

    // reference:
    // https://docs.aws.amazon.com/keyspaces/latest/devguide/consistency.html#UnsupportedConsistency
    public static List<ConsistencyLevel> UNSUPPORTED_CONSISTENCY_LEVELS =
            Arrays.asList(
                    DefaultConsistencyLevel.EACH_QUORUM,
                    DefaultConsistencyLevel.QUORUM,
                    DefaultConsistencyLevel.ALL,
                    DefaultConsistencyLevel.TWO,
                    DefaultConsistencyLevel.THREE,
                    DefaultConsistencyLevel.ANY,
                    DefaultConsistencyLevel.SERIAL,
                    DefaultConsistencyLevel.LOCAL_SERIAL);
}
