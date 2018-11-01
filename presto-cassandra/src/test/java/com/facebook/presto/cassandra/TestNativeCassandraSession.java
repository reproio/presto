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
package com.facebook.presto.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestNativeCassandraSession
{
    private static final String KEYSPACE = "test_native_cassandra_session_keyspace";
    private static final int FILTER_PARTITION_COUNT = 5;
    private static final int EXISTING_PARTITION_COUNT = 4;
    private static final int CLUSTERING_KEY_COUNT = 3;

    private CassandraSession session;

    @BeforeClass
    public void setUp() throws Exception
    {
        EmbeddedCassandra.start();
        session = EmbeddedCassandra.getSession();
        createKeyspace(session, KEYSPACE);
    }

    @Test
    public void testGetPartitionsFromSingleParitionKeyTable()
    {
        NativeCassandraSession nativeSession = buildNativeSession(false);
        String tableName = "single_part_key_table";
        CassandraTable table = createSinglePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildSinglePartitionKeysList();
        List<CassandraPartition> partitions = nativeSession.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), EXISTING_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromSinglePartitionKeyTableWithSkipPartitionCheck()
    {
        NativeCassandraSession nativeSession = buildNativeSession(true);
        String tableName = "single_part_key_with_skip_partition_check_table";
        CassandraTable table = createSinglePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildSinglePartitionKeysList();
        List<CassandraPartition> partitions = nativeSession.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), FILTER_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromMultipleParitionKeyTable()
    {
        NativeCassandraSession nativeSession = buildNativeSession(false);
        String tableName = "multi_part_key_table";
        CassandraTable table = createMultiplePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildMultiplePartitionKeysList();
        List<CassandraPartition> partitions = nativeSession.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), EXISTING_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromMultiplePartitionKeyTableWithSkipPartitionCheck()
    {
        NativeCassandraSession nativeSession = buildNativeSession(true);
        String tableName = "multi_part_key_with_skip_partition_check_table";
        CassandraTable table = createMultiplePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildMultiplePartitionKeysList();
        List<CassandraPartition> partitions = nativeSession.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), FILTER_PARTITION_COUNT * FILTER_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    private NativeCassandraSession buildNativeSession(boolean skipPartitionCheck)
    {
        return new NativeCassandraSession(
                "TestNativeCassandraSession",
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                EmbeddedCassandra.getCluster(),
                new Duration(1, MINUTES),
                skipPartitionCheck);
    }

    private ImmutableList<Set<Object>> buildSinglePartitionKeysList()
    {
        ImmutableSet.Builder<Object> partitionColumnValues = ImmutableSet.builder();
        for (int i = 0; i < FILTER_PARTITION_COUNT; i++) {
            partitionColumnValues.add((long) i);
        }
        return ImmutableList.of(partitionColumnValues.build());
    }

    private CassandraTable createSinglePartitionKeyTable(String tableName)
    {
        session.execute(format("CREATE TABLE %s.%s (partition_key1 bigint, clustering_key1 bigint, PRIMARY KEY (partition_key1, clustering_key1))", KEYSPACE, tableName));
        for (int i = 0; i < EXISTING_PARTITION_COUNT; i++) {
            for (int j = 0; j < CLUSTERING_KEY_COUNT; j++) {
                session.execute(format("INSERT INTO %s.%s (partition_key1, clustering_key1) VALUES (%d, %d)", KEYSPACE, tableName, i, j));
            }
        }

        CassandraColumnHandle col1 = new CassandraColumnHandle("cassandra", "partition_key1", 1, CassandraType.BIGINT, null, true, false, false, false);
        CassandraColumnHandle col2 = new CassandraColumnHandle("cassandra", "clustering_key1", 2, CassandraType.BIGINT, null, false, true, false, false);
        return new CassandraTable(new CassandraTableHandle("cassandra", KEYSPACE, tableName), ImmutableList.of(col1, col2));
    }

    private ImmutableList<Set<Object>> buildMultiplePartitionKeysList()
    {
        ImmutableSet.Builder<Object> col1Values = ImmutableSet.builder();
        ImmutableSet.Builder<Object> col2Values = ImmutableSet.builder();
        for (int i = 0; i < FILTER_PARTITION_COUNT; i++) {
            col1Values.add((long) i);
            col2Values.add(Slices.utf8Slice(Integer.toString(i)));
        }
        return ImmutableList.of(col1Values.build(), col2Values.build());
    }

    private CassandraTable createMultiplePartitionKeyTable(String tableName)
    {
        session.execute(format("CREATE TABLE %s.%s (partition_key1 bigint, partition_key2 text, clustering_key1 bigint, PRIMARY KEY ((partition_key1, partition_key2), clustering_key1))", KEYSPACE, tableName));
        for (int i = 0; i < EXISTING_PARTITION_COUNT; i++) {
            for (int j = 0; j < CLUSTERING_KEY_COUNT; j++) {
                session.execute(format("INSERT INTO %s.%s (partition_key1, partition_key2, clustering_key1) VALUES (%d, '%s', %d)", KEYSPACE, tableName, i, Integer.toString(i), j));
            }
        }

        CassandraColumnHandle col1 = new CassandraColumnHandle("cassandra", "partition_key1", 1, CassandraType.BIGINT, null, true, false, false, false);
        CassandraColumnHandle col2 = new CassandraColumnHandle("cassandra", "partition_key2", 2, CassandraType.TEXT, null, true, false, false, false);
        CassandraColumnHandle col3 = new CassandraColumnHandle("cassandra", "clustering_key1", 3, CassandraType.BIGINT, null, false, true, false, false);
        return new CassandraTable(new CassandraTableHandle("cassandra", KEYSPACE, tableName), ImmutableList.of(col1, col2, col3));
    }
}
