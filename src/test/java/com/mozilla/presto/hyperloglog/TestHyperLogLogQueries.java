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

package com.mozilla.presto.hyperloglog;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.spi.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestHyperLogLogQueries
        extends AbstractTestQueryFramework
{
    public TestHyperLogLogQueries()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testHyperLogLogMerge()
            throws Exception
    {
        assertQuery("select cardinality(merge(hll_create(v, 10))) from (values ('foo'), ('bar'), ('foo')) as t(v)", "select 2");
    }

    @Test
    public void testHyperLogLogGroupMerge()
            throws Exception
    {
        assertQuery("select k, cardinality(merge(hll_create(v, 10))) from (values ('US', 'foo'), ('US', 'bar'), ('IT', 'foo')) as t(k, v) group by k",
                "select * from (values ('US', 2), ('IT', 1))");
    }

    @Test
    public void testHyperLogLogCast()
        throws Exception
    {
        assertQuery("select cardinality(cast(cast(hll_create('foo', 4) as varbinary) as HLL))", "select 1");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        HyperLogLogPlugin plugin = new HyperLogLogPlugin();
        for (Type type : plugin.getTypes()) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getParametricTypes()) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }

        localQueryRunner.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));

        return localQueryRunner;
    }
}
