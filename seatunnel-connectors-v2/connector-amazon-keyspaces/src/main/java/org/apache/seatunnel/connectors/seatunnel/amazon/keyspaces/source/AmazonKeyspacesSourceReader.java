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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.config.AmazonKeyspacesConfig;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.client.AmazonKeyspacesClient;
import org.apache.seatunnel.connectors.seatunnel.amazon.keyspaces.serialize.RowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.IOException;

public class AmazonKeyspacesSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final AmazonKeyspacesConfig keyspacesConfig;
    private final SingleSplitReaderContext readerContext;

    private CqlSession session;

    public AmazonKeyspacesSourceReader(
            AmazonKeyspacesConfig keyspacesConfig, SingleSplitReaderContext readerContext) {
        this.keyspacesConfig = keyspacesConfig;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        session = AmazonKeyspacesClient.buildCqlSession(keyspacesConfig);
    }

    @Override
    public void close() throws IOException {
        if (session != null) {
            session.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

        ResultSet rs =
                session.execute(
                        SimpleStatement.builder(keyspacesConfig.getCql())
                                .setConsistencyLevel(keyspacesConfig.getConsistencyLevel())
                                .build());
        rs.forEach(row -> output.collect(RowDeserializer.deserialize(row)));
    }
}
