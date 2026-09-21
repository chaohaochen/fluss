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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster-level reproduction of the ISR-deadlock-and-self-heal chain observed in production.
 *
 * <p>A follower falls behind before a batch-length corruption in an inactive leader segment makes
 * the leader serve a later batch than the follower expects. The follower rejects that batch with
 * {@link org.apache.fluss.exception.OutOfOrderSequenceException} and remains outside the ISR until
 * periodic writer-ID expiration evicts its stale writer state. The follower then accepts the later
 * batch, catches up with a silent offset gap, and rejoins the ISR.
 */
class IsrDeadlockSelfHealITCase {

    private static final int ALL_ACKS = -1;
    private static final int RECORDS_PER_BATCH = DATA1.size();
    private static final long WRITER_ID = 100L;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(createClusterConfig())
                    .build();

    @Test
    void testIsrDeadlockRecoversAfterWriterExpiration() throws Exception {
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);

        int leaderServerId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        int followerServerId =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().stream()
                        .filter(node -> node.id() != leaderServerId)
                        .findFirst()
                        .get()
                        .id();

        // 1. Write b0 (writerId=100, seq=0, acks=-1) so all replicas hold offsets [0, 10).
        appendToLeaderLog(leaderServerId, tableBucket, createBatch(0));
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        Replica followerReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, followerServerId);
        assertThat(leaderReplica.getLocalLogEndOffset()).isEqualTo(RECORDS_PER_BATCH);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(followerReplica.getLocalLogEndOffset())
                                .isEqualTo(RECORDS_PER_BATCH));

        // 2. Stop the target follower, leaving the leader and other follower in the ISR.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(followerServerId);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tableBucket, followerServerId);

        // 3. Append b1 (seq=1) and b2 (seq=2) to the first segment, roll, then append b3 (seq=3).
        //    The two remaining replicas satisfy min-ISR=2 and advance the high watermark to 40.
        appendToLeaderLog(leaderServerId, tableBucket, createBatch(1));
        appendToLeaderLog(leaderServerId, tableBucket, createBatch(2));
        leaderReplica.getLogTablet().roll(Optional.empty());
        appendToLeaderLog(leaderServerId, tableBucket, createBatch(3));
        assertThat(leaderReplica.getLocalLogEndOffset()).isEqualTo(4L * RECORDS_PER_BATCH);
        assertThat(leaderReplica.getLogTablet().getHighWatermark())
                .isEqualTo(4L * RECORDS_PER_BATCH);
        leaderReplica.getLogTablet().flush(true);

        // 4. Corrupt b1's length field in the inactive segment so reading at offset 10 skips to b3.
        corruptBatchLengthAtOffset(leaderReplica, RECORDS_PER_BATCH);

        // 5. The restarted follower rejects b3 (seq=3) and stays at offset 10, outside the ISR.
        FLUSS_CLUSTER_EXTENSION.startTabletServer(followerServerId);
        Replica restartedFollower =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, followerServerId);

        // Allow several fetch retries while remaining below the 30-second expiration threshold.
        Thread.sleep(2000L);
        assertThat(restartedFollower.getLocalLogEndOffset()).isEqualTo(RECORDS_PER_BATCH);
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr.isr()).doesNotContain(followerServerId);

        // 6. Writer-ID expiration lets the follower accept b3, jump to offset 40, and rejoin.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(restartedFollower.getLocalLogEndOffset())
                                .isEqualTo(4L * RECORDS_PER_BATCH));
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(tableBucket, followerServerId);
    }

    private static MemoryLogRecords createBatch(int batchSequence) throws Exception {
        return genMemoryLogRecordsWithWriterId(DATA1, WRITER_ID, batchSequence, 0L);
    }

    private static void appendToLeaderLog(
            int leaderServerId, TableBucket tableBucket, MemoryLogRecords records)
            throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> resultFuture = new CompletableFuture<>();
        FLUSS_CLUSTER_EXTENSION
                .getTabletServerById(leaderServerId)
                .getReplicaManager()
                .appendRecordsToLog(
                        30_000,
                        ALL_ACKS,
                        Collections.singletonMap(tableBucket, records),
                        null,
                        resultFuture::complete);
        ProduceLogResultForBucket appendResult = resultFuture.get().get(0);
        if (appendResult.failed()) {
            throw appendResult.getError().exception();
        }
    }

    /**
     * Overwrites the 4-byte little-endian length field of the batch that owns {@code offset} with a
     * huge positive value, so {@code FileLogInputStream.nextBatch()} treats the batch as
     * overrunning the file and returns null.
     */
    private static void corruptBatchLengthAtOffset(Replica leaderReplica, long offset)
            throws Exception {
        List<LogSegment> segments = leaderReplica.getLogTablet().logSegments();
        assertThat(segments.size()).isGreaterThanOrEqualTo(2);
        LogSegment inactiveSegment = segments.get(0);
        int batchPosition = inactiveSegment.translateOffset(offset).getPosition();
        File logFile = inactiveSegment.getFileLogRecords().file();
        // LogRecordBatchFormat: baseOffset(8) then length(4). length is stored little-endian.
        try (RandomAccessFile logFileAccess = new RandomAccessFile(logFile, "rw")) {
            logFileAccess.seek(batchPosition + 8L);
            logFileAccess.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F});
        }
    }

    private static Configuration createClusterConfig() {
        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        configuration.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        configuration.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofSeconds(30));
        configuration.set(ConfigOptions.WRITER_ID_EXPIRATION_CHECK_INTERVAL, Duration.ofSeconds(1));
        return configuration;
    }
}
