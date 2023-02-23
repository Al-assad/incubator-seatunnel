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

package org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesConfig;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.exception.AmazonKeyspacesException;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.CQL;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.DATA_CENTER;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.IAM_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.IAM_USER;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.KEYSPACE;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesOption.SSL_TRUSTSTORE_PATH;

@AutoService(SeaTunnelSource.class)
public class AmazonKeyspacesSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType rowType;
    private AmazonKeyspacesConfig config;

    @Override
    public String getPluginName() {
        return "AmazonKeyspaces";
    }

    @Override
    public void prepare(Config pluginConfig) throws AmazonKeyspacesException {

        // Config items verification
        CheckResult checkRs =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        DATA_CENTER.key(),
                        IAM_USER.key(),
                        IAM_PASSWORD.key(),
                        SSL_TRUSTSTORE_PATH.key(),
                        SSL_TRUSTSTORE_PASSWORD.key(),
                        KEYSPACE.key(),
                        CQL.key());

        if (checkRs.isSuccess()) {
            throw new AmazonKeyspacesException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, checkRs.getMsg()));
        }

        config = AmazonKeyspacesConfig.of(pluginConfig);

        if (AmazonKeyspacesOption.UNSUPPORTED_CONSISTENCY_LEVELS.contains(
                config.getConsistencyLevel())) {
            throw new AmazonKeyspacesException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: Unsupported consistency level %s",
                            getPluginName(), PluginType.SOURCE, config.getConsistencyLevel()));
        }

        // init cql session
        //        try( CqlSession session = buildCqlSession()) {
        //
        //        }

    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AmazonKeyspacesSourceReader(config, readerContext);
    }
}
