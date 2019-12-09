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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

public class TestCassandraTypeWithTypeArguments
{
    @Test
    public void testJsonMapEncoding()
    {
        assertTrue(isValidJson(CassandraTypeWithTypeArguments.buildArrayValue(
                Lists.newArrayList("one", "two", "three\""), new CassandraTypeWithTypeArguments(CassandraType.VARCHAR, ImmutableList.of()))));
        assertTrue(isValidJson(CassandraTypeWithTypeArguments.buildArrayValue(
                Lists.newArrayList(1, 2, 3), new CassandraTypeWithTypeArguments(CassandraType.INT, ImmutableList.of()))));
        assertTrue(isValidJson(CassandraTypeWithTypeArguments.buildArrayValue(
                Lists.newArrayList(100000L, 200000000L, 3000000000L), new CassandraTypeWithTypeArguments(CassandraType.BIGINT, ImmutableList.of()))));
        assertTrue(isValidJson(CassandraTypeWithTypeArguments.buildArrayValue(
                Lists.newArrayList(1.0, 2.0, 3.0), new CassandraTypeWithTypeArguments(CassandraType.DOUBLE, ImmutableList.of()))));
    }

    private static void continueWhileNotNull(JsonParser parser, JsonToken token)
            throws IOException
    {
        if (token != null) {
            continueWhileNotNull(parser, parser.nextToken());
        }
    }

    private static boolean isValidJson(String json)
    {
        boolean valid = false;
        try {
            JsonParser parser = new ObjectMapper().getFactory()
                    .createParser(json);
            continueWhileNotNull(parser, parser.nextToken());
            valid = true;
        }
        catch (IOException ignored) {
        }
        return valid;
    }
}
