package org.embulk.filter.distinct;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.embulk.filter.distinct.DistinctFilterPlugin.PluginTask;
import static org.embulk.spi.FilterPlugin.Control;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestDistinctFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static JsonParser jsonParser = new JsonParser();
    private Schema schema;
    private DistinctFilterPlugin plugin;

    // http://stackoverflow.com/questions/3301635/change-private-static-final-field-using-java-reflection
    private static void setFinalStaticVariable(Field field, Object newValue)
            throws IllegalAccessException, NoSuchFieldException
    {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    private Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            builder.add((String) nameAndTypes[i], (Type) nameAndTypes[i + 1]);
        }
        return builder.build();
    }

    private ConfigSource loadConfigFromYaml(String yaml)
    {
        ConfigLoader loader = new ConfigLoader(runtime.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Before
    public void setupDefault()
    {
        schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
        plugin = new DistinctFilterPlugin();
    }

    @Before
    public void resetStatic()
            throws IllegalAccessException, NoSuchFieldException
    {
        Field set = DistinctFilterPageOutput.class.getDeclaredField("set");
        setFinalStaticVariable(set, Sets.<List<Object>>newConcurrentHashSet());
    }

    @Test
    public void testConfigure()
    {
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0, _c1]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals(Lists.newArrayList("_c0", "_c1"), task.getDistinctColumnNames());
    }

    @Test
    public void testConfigureInjectedTask()
    {
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0, _c1]\n";

        ConfigSource config = loadConfigFromYaml(yaml);

        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                PluginTask task = taskSource.loadTask(PluginTask.class);

                List<Column> columns = Lists.newArrayList();
                columns.add(new Column(0, "_c0", Types.STRING));
                columns.add(new Column(1, "_c1", Types.STRING));

                assertEquals(columns, task.getDistinctColumns());
            }
        });
    }

    @Test
    public void testDistinctBySingleColumn()
    {
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "a", "a",  // row: 1
                            "a", "a",  // row: 2
                            "a", "b",  // row: 3
                            "b", "b",  // row: 4
                            "b", "a",  // row: 5
                            "b", "b",  // row: 6
                            null, "a", // row: 7
                            null, "b"  // row: 8
                            )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals("a", record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals("b", record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }

    @Test
    public void testDistinctByMultipleColumns()
    {
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0, _c1]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "a", "a",  // row: 1
                            "a", "a",  // row: 2
                            "a", "b",  // row: 3
                            "b", "b",  // row: 4
                            "b", "a",  // row: 5
                            "b", "b",  // row: 6
                            null, "a", // row: 7
                            null, "b"  // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(6, records.size());

                Object[] record1 = records.get(0);
                assertEquals("a", record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals("a", record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertEquals("b", record3[0]);
                assertEquals("b", record3[1]);

                Object[] record4 = records.get(3);
                assertEquals("b", record4[0]);
                assertEquals("a", record4[1]);

                Object[] record5 = records.get(4);
                assertNull(record5[0]);
                assertEquals("a", record5[1]);

                Object[] record6 = records.get(5);
                assertNull(record6[0]);
                assertEquals("b", record6[1]);
            }
        });
    }

    @Test
    public void testDistinctByLongColumn()
    {
        schema = schema("_c0", Types.LONG, "_c1", Types.STRING);
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            1L, "a",   // row: 1
                            1L, "a",   // row: 2
                            1L, "b",   // row: 3
                            2L, "b",   // row: 4
                            2L, "a",   // row: 5
                            2L, "b",   // row: 6
                            null, "a", // row: 7
                            null, "b"  // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals(1L, record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals(2L, record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }

    @Test
    public void testDistinctByDoubleColumn()
    {
        schema = schema("_c0", Types.DOUBLE, "_c1", Types.STRING);
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            1.1, "a",  // row: 1
                            1.1, "a",  // row: 2
                            1.1, "b",  // row: 3
                            2.2, "b",  // row: 4
                            2.2, "a",  // row: 5
                            2.2, "b",  // row: 6
                            null, "a", // row: 7
                            null, "b"  // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals(1.1, record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals(2.2, record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }

    @Test
    public void testDistinctByBooleanColumn()
    {
        schema = schema("_c0", Types.BOOLEAN, "_c1", Types.STRING);
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            true, "a",  // row: 1
                            true, "a",  // row: 2
                            true, "b",  // row: 3
                            false, "b", // row: 4
                            false, "a", // row: 5
                            false, "b", // row: 6
                            null, "a",  // row: 7
                            null, "b"   // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals(true, record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals(false, record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }


    @Test
    public void testDistinctByTimestampColumn()
    {
        schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.STRING);
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            Timestamp.ofEpochSecond(1), "a", // row: 1
                            Timestamp.ofEpochSecond(1), "a", // row: 2
                            Timestamp.ofEpochSecond(1), "b", // row: 3
                            Timestamp.ofEpochSecond(2), "b", // row: 4
                            Timestamp.ofEpochSecond(2), "a", // row: 5
                            Timestamp.ofEpochSecond(2), "b", // row: 6
                            null, "a",                       // row: 7
                            null, "b"                        // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals(Timestamp.ofEpochSecond(1), record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals(Timestamp.ofEpochSecond(2), record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }


    @Test
    public void testDistinctByJsonColumn()
    {
        schema = schema("_c0", Types.JSON, "_c1", Types.STRING);
        String yaml = "" +
                "type: distinct\n" +
                "columns: [_c0]\n";

        final String json1 = "{\"a\":1,\"b\":\"b\"}";
        final String json2 = "{\"a\":2,\"b\":\"b\"}";

        ConfigSource config = loadConfigFromYaml(yaml);
        plugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput output = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, schema, outputSchema, output)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            jsonParser.parse(json1), "a", // row: 1
                            jsonParser.parse(json1), "a", // row: 2
                            jsonParser.parse(json1), "b", // row: 3
                            jsonParser.parse(json2), "b", // row: 4
                            jsonParser.parse(json2), "a", // row: 5
                            jsonParser.parse(json2), "b", // row: 6
                            null, "a",                    // row: 7
                            null, "b"                     // row: 8
                    )
                            ) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, output.pages);
                assertEquals(3, records.size());

                Object[] record1 = records.get(0);
                assertEquals(jsonParser.parse(json1), record1[0]);
                assertEquals("a", record1[1]);

                Object[] record2 = records.get(1);
                assertEquals(jsonParser.parse(json2), record2[0]);
                assertEquals("b", record2[1]);

                Object[] record3 = records.get(2);
                assertNull(record3[0]);
                assertEquals("a", record3[1]);
            }
        });
    }
}
