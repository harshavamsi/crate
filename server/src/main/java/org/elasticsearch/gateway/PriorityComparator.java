/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import java.util.Comparator;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

/**
 * A comparator that compares ShardRouting based on its creation date (index.creation_date),
 * We try to recover shards that are newer first.
 */
public abstract class PriorityComparator implements Comparator<ShardRouting> {

    @Override
    public final int compare(ShardRouting o1, ShardRouting o2) {
        final String o1Index = o1.getIndexName();
        final String o2Index = o2.getIndexName();
        if (o1Index.equals(o2Index) == false) {
            final Settings settingsO1 = getIndexSettings(o1.index());
            final Settings settingsO2 = getIndexSettings(o2.index());
            int cmp = Long.compare(timeCreated(settingsO2), timeCreated(settingsO1));
            if (cmp == 0) {
                return o2Index.compareTo(o1Index);
            }
        }
        return 0;
    }

    private static long timeCreated(Settings settings) {
        return settings.getAsLong(IndexMetadata.SETTING_CREATION_DATE, -1L);
    }

    protected abstract Settings getIndexSettings(Index index);

    /**
     * Returns a PriorityComparator that uses the RoutingAllocation index metadata to access the index setting per index.
     */
    public static PriorityComparator getAllocationComparator(final RoutingAllocation allocation) {
        return new PriorityComparator() {
            @Override
            protected Settings getIndexSettings(Index index) {
                IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(index);
                return indexMetadata.getSettings();
            }
        };
    }
}
