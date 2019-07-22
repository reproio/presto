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
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static io.airlift.tpch.TpchTable.ORDERS;

@Test
public class TestKafkaIntegrationSmokeTestWithInternalColumns
        extends AbstractTestQueryFramework
{
    private final EmbeddedKafka embeddedKafka;

    public TestKafkaIntegrationSmokeTestWithInternalColumns()
            throws Exception
    {
        this(createEmbeddedKafka());
    }

    public TestKafkaIntegrationSmokeTestWithInternalColumns(EmbeddedKafka embeddedKafka)
    {
        super(() -> createKafkaQueryRunner(embeddedKafka, ImmutableList.of(ORDERS), false));
        this.embeddedKafka = embeddedKafka;
    }

    @Test
    public void testPartitionIdPredicate()
    {
        assertQuery(
                "SELECT _partition_id, min(_partition_offset), max(_partition_offset) FROM orders WHERE _partition_id = 1 GROUP BY _partition_id",
                "VALUES (1, 0, 7499)");
        assertQuery(
                "SELECT _partition_id, min(_partition_offset), max(_partition_offset) FROM orders WHERE _partition_id BETWEEN 0 AND 1 AND _partition_offset >= 100 AND _partition_offset <= 1000 GROUP BY _partition_id",
                "VALUES (0, 100, 1000), (1, 100, 1000)");
    }

    @Test
    public void testPartitionOffsetPredicate()
    {
        assertQuery(
                "SELECT _partition_id, min(_partition_offset), max(_partition_offset) FROM orders WHERE _partition_offset >= 10 AND _partition_offset < 7500 GROUP BY _partition_id",
                "VALUES (0, 10, 7499), (1, 10, 7499)");
        assertQuery(
                "SELECT _partition_id, min(_partition_offset), max(_partition_offset) FROM orders WHERE _partition_offset BETWEEN 100 AND 1000 GROUP BY _partition_id",
                "VALUES (0, 100, 1000), (1, 100, 1000)");
        assertQuery(
                "SELECT _partition_id, min(_partition_offset), max(_partition_offset) FROM orders WHERE _partition_id = 1 AND _partition_offset BETWEEN 100 AND 1000 GROUP BY _partition_id",
                "VALUES (1, 100, 1000)");
        assertQuery(
                "SELECT _partition_id, _partition_offset FROM orders WHERE (_partition_offset >= 100 AND _partition_offset < 102) OR (_partition_id = 1 AND _partition_offset > 7498) OR (_partition_offset BETWEEN 200 AND 201)",
                "VALUES (0, 100), (1, 100), (0, 101), (1, 101), (0, 200), (1, 200), (0, 201), (1, 201), (1, 7499)");
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        embeddedKafka.close();
    }
}
