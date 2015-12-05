package org.embulk.filter.distinct;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

public class DistinctFilterPlugin
        implements FilterPlugin
{
    private static Logger logger = Exec.getLogger(DistinctFilterPlugin.class);
    private static Set<String> filter = Sets.newConcurrentHashSet();

    public interface PluginTask
            extends Task
    {
        @Config("keys")
        public List<String> getKeys();
//        // configuration option 1 (required integer)
//        @Config("option1")
//        public int getOption1();
//
//        // configuration option 2 (optional string, null is not allowed)
//        @Config("option2")
//        @ConfigDefault("\"myvalue\"")
//        public String getOption2();
//
//        // configuration option 3 (optional string, null is allowed)
//        @Config("option3")
//        @ConfigDefault("null")
//        public Optional<String> getOption3();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final List<Column> distinctColumns = Lists.newArrayList();
        for (String key : task.getKeys()) {
            for (Column column : outputSchema.getColumns()) {
                if (key.contentEquals(column.getName())) {
                    distinctColumns.add(column);
                }
            }
        }

        if (distinctColumns.isEmpty()) {
            throw new ConfigException("input schema does not have keys for distinct.");
        }

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);
            private final String delimiter = "___";

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    StringBuilder sb = new StringBuilder();
                    for (Column distinctColumn : distinctColumns) {
                        if (!pageReader.isNull(distinctColumn)) {
                            if (Types.BOOLEAN.equals(distinctColumn.getType())) {
                                sb.append(pageReader.getBoolean(distinctColumn));
                            }
                            else if (Types.DOUBLE.equals(distinctColumn.getType())) {
                                sb.append(pageReader.getDouble(distinctColumn));
                            }
                            else if (Types.LONG.equals(distinctColumn.getType())) {
                                sb.append(pageReader.getLong(distinctColumn));
                            }
                            else if (Types.STRING.equals(distinctColumn.getType())) {
                                sb.append(pageReader.getString(distinctColumn));
                            }
                            else if (Types.TIMESTAMP.equals(distinctColumn.getType())) {
                                sb.append(pageReader.getTimestamp(distinctColumn));
                            }
                            else {
                                throw new RuntimeException("unsupported type: " + distinctColumn.getType());
                            }
                        }
                        sb.append(delimiter);
                    }

                    if (filter.add(sb.toString()
                    )) {
                        outputSchema.visitColumns(visitor);
                        pageBuilder.addRecord();
                    }
                }
            }

            class ColumnVisitorImpl implements ColumnVisitor
            {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder) {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(outputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setLong(outputColumn, pageReader.getLong(outputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setDouble(outputColumn, pageReader.getDouble(outputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setString(outputColumn, pageReader.getString(outputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(outputColumn));
                    }
                }
            }
        };
    }
}
