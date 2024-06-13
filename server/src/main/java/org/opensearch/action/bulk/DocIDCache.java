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

package org.opensearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ThreadInterruptedException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.util.concurrent.ConcurrentCollections;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
/**
 * Cache Per Shard for Doc IDs
 * Pre-Generate and cache (n x m) doc IDs in HashMap, with n = # docRequests, m = # routingShards on cluster
 *
 */
class DocIDCache {
    private static final Logger logger = LogManager.getLogger(DocIDCache.class);

    static final int MAX_REFILL_BATCH_SIZE = 5000;
    private final ConcurrentMap<DocIDCacheKey, DocIDQueue> docIdsByShard;

    /**
     * Per-ShardID bounded Queue, uses semaphore to restrict the queue size b/w multiple consumers
     *
     */
    static final class DocIDQueue implements Closeable {
        private volatile boolean closed;
        private final Semaphore permits = new Semaphore(MAX_REFILL_BATCH_SIZE);
        private final Queue<String> queue = new ConcurrentLinkedQueue<>();

        private Boolean acquire() {
            return permits.tryAcquire();
            /* if (closed) {
                permits.release(permitCount);
                throw new AlreadyClosedException("queue is closed");
            }*/
        }

        void addDocIDs(String[] docIDs) {
            // Only add if available permits are less than 50% batch size capacity
            final long startTimeNanos = System.nanoTime();
            try {
                permits.acquire(docIDs.length);
                Collections.addAll(queue, docIDs);
                logger.info("Got docIDs Time taken: {}", System.nanoTime() - startTimeNanos, docIDs.length);
            } catch (InterruptedException e) {
                logger.error("Error adding docIDs to DocID store queue");
            }
        }

        String processDocID() {
            String docID;
            if ((docID = queue.poll()) != null) {
                permits.release();
            }
            return docID;
        }

        @Override
        public synchronized void close() {
            assert !closed : "we should never close this twice";
            closed = true;
            try {
                permits.acquire(MAX_REFILL_BATCH_SIZE);
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(e);
            }
            try {
                assert MAX_REFILL_BATCH_SIZE - permits.availablePermits() > 0 : "must acquire a permit before consuming docID";
                queue.clear();
            } finally {
                permits.release(MAX_REFILL_BATCH_SIZE);
            }
        }
    }

    /**
     * Key for Doc IDs Cache
     *
     */
    static class DocIDCacheKey {
        private final String indexName;
        private final int shardId;

        DocIDCacheKey(String indexName, int shardId) {
            this.indexName = indexName;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof DocIDCacheKey) {
                DocIDCacheKey key = (DocIDCacheKey) o;
                return indexName.equals(key.indexName) && shardId == key.shardId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 31 * indexName.hashCode() + shardId;
        }

        @Override
        public String toString() {
            return "[" + indexName + "][" + shardId + "]";
        }
    }

    private final int cacheCapacity;
    private final int perEntryCapacity;

    DocIDCache(int capacity, int initialFill) {
        this.docIdsByShard = ConcurrentCollections.newConcurrentMap();
        this.cacheCapacity = capacity;
        this.perEntryCapacity = initialFill;
        logger.info("Initializing  DocID Store with initial perShard-Entry Capacity: {}", this.perEntryCapacity);
    }

    /**
     * Periodically refill the cache.
     * Make refill smart, to optimize the memory usage - generate docIDs based on recent throughput seen on the active shard.
     */
    public void refill(ClusterService clusterService) {
        final long startTimeNanos = System.nanoTime();
        try {
            if (clusterService.state() == null) return;
            Metadata metadata = clusterService.state().metadata();
            final String[] concreteIndices = Arrays.stream(metadata.getConcreteAllIndices())
                .filter(index -> !index.startsWith("."))
                .toArray(String[]::new);
            logger.info("Inside refill - concreteIndices: {}", Arrays.asList(concreteIndices));
            for (String concreteIndex : concreteIndices) {
                final int numShards = metadata.index(concreteIndex).getNumberOfShards();

                IntStream.range(0, numShards).forEach(shardId -> {
                    final DocIDCacheKey cacheKey = new DocIDCacheKey(concreteIndex, shardId);
                    docIdsByShard.putIfAbsent(cacheKey, new DocIDQueue());
                    docIdsByShard.computeIfPresent(cacheKey, (key, prevDocIds) -> {
                        logger.info("Invoking refill on concreteIndex: {} and shard: {} and queue available permits: {}", concreteIndex, shardId, prevDocIds.permits.availablePermits());
                        // TODO: Make this more intelligent - refill on pro-rated shard throughput basis
                        if (prevDocIds.permits.availablePermits() >= perEntryCapacity/2 ) {
                            String[] docIDs = generateDocIdForShard(metadata.index(concreteIndex), shardId, prevDocIds.permits.availablePermits(), numShards);
                            logger.info("Generated #docIDs: {} and Time taken: {}", docIDs.length, System.nanoTime() - startTimeNanos);
                            if (docIDs.length > 0) {
                                prevDocIds.addDocIDs(docIDs);
                                logger.info("Post Permit addition: {} on concreteIndex: {} and shard: {}, time taken: {}", prevDocIds.queue.size(), concreteIndex, shardId, (System.nanoTime() - startTimeNanos));
                            }
                        } else {
                            logger.info("ConcreteIndex: {} and shard: {} already have {} permits. No refill required.", concreteIndex, shardId, prevDocIds.permits.availablePermits());
                        }
                        return prevDocIds;
                    });
                });
                logger.info("Completed the refill for all indices");
            }
        } catch (Exception e) {
            logger.error("Failed to refill the DocID Store: ", e);
        }
    }

    private static int getFillCount(int delta, int numShards) {
        return numShards * delta;
    }

    private List<String> generateDocIds(int docCount) {
        return IntStream.range(0, docCount).mapToObj(id -> UUIDs.base64UUID()).collect(Collectors.toCollection(LinkedList::new));
    }

    private String[] generateDocIdForShard(IndexMetadata indexMetadata, int shardId, int availableSlots, int numShards) {
        final int docCount = getFillCount(availableSlots, numShards);
        final List<String> candidateDocIDs = IntStream.range(0, docCount).mapToObj(id -> UUIDs.base64UUID()).collect(Collectors.toCollection(LinkedList::new));
        return candidateDocIDs.stream()
            .filter(docId -> (OperationRouting.generateShardId(indexMetadata, docId, null) == shardId))
            .limit(availableSlots)
            .toArray(String[]::new);
    }

    public String consumeDocId(DocIDCacheKey key) {
        if (docIdsByShard.get(key) != null && docIdsByShard.get(key).queue.size() > 0) {
            return docIdsByShard.get(key).processDocID();
        }
        return null;
    }

    /*private void evict() {
        // In case the cache entries breach the capacity, evict delta entries
        int delta = docIdsByShard.size() - cacheCapacity;
        if (delta > 0) {
            final Iterator<Map.Entry<DocIDCacheKey, DocIDQueue<String>>> iterator = docIdsByShard.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<DocIDCacheKey, LinkedList<String>> entry = iterator.next();
                if (refCountMap.get(entry.getKey()).intValue() < getAverageRefCount()) {
                    docIdsByShard.remove(entry.getKey());
                    delta--;
                }
            }
            // TODO: Run another pass to remove entries based on LRU (store timestamp in Cache Value obj)
        }
    }
    private double getAverageRefCount() {
        OptionalDouble averageRefCount = refCountMap.values().stream()
            .mapToDouble(LongAdder::doubleValue)
            .average();
        return averageRefCount.isPresent() ? averageRefCount.getAsDouble() : 0;
    }*/
}

