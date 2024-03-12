/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.object.HasToString.hasToString;

public class NodeIndicesStatsTests extends OpenSearchTestCase {

    public void testInvalidLevel() {
        CommonStats oldStats = new CommonStats();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats requestStats = new SearchRequestStats(clusterSettings);
        final NodeIndicesStats stats = new NodeIndicesStats(oldStats, Collections.emptyMap(), requestStats, false);
        final String level = randomAlphaOfLength(16);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("level", level));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.toXContent(null, params));
        assertThat(
            e,
            hasToString(containsString("level parameter must be one of [indices] or [node] or [shards] but was [" + level + "]"))
        );
    }

    public void testPartialResultStatus() throws IOException {
        CommonStats oldStats = new CommonStats();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats requestStats = new SearchRequestStats(clusterSettings);
        final NodeIndicesStats stats = new NodeIndicesStats(oldStats, Collections.emptyMap(), requestStats, true);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(NodeIndicesStats.Fields.PARTIAL_RESULTS, "true"));
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        final XContentBuilder response = stats.toXContent(builder, params);
        String expected = "{\n"
            + "  \"partial_results\" : true"
            + ",\n"
            + "  \"indices\" : { }\n"
            + "}";
        assertEquals(expected, response.toString());
    }

    public void testSerialization() throws IOException {
        NodeIndicesStats expectedStats = new NodeIndicesStats(new CommonStats(), Collections.emptyMap(), null, true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            expectedStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                NodeIndicesStats stats = new NodeIndicesStats(in);
                assertEquals(expectedStats.getPartialResultStatus(), stats.getPartialResultStatus());
            }
        }
    }
}
