package org.embulk.filter.distinct;

import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.List;

public class DistinctFilterPlugin
        implements FilterPlugin
{
    private final static Logger logger = Exec.getLogger(DistinctFilterPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public List<String> getDistinctColumnNames();

        @ConfigInject
        public void setDistinctColumns(List<Column> columns);
        public List<Column> getDistinctColumns();

        // is used to concatenate multiple values for generating the key to distinguish records.
        @Config("delimiter")
        @ConfigDefault("\"--\"")
        public String getDelimiter();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        List<Column> distinctColumns = convertNameToColumn(inputSchema, task.getDistinctColumnNames());
        task.setDistinctColumns(distinctColumns);

        if (task.getDistinctColumns().isEmpty()) {
            throw new ConfigException(
                    "inputSchema does not have any columns you configured.");
        }
        else {
            logger.debug("distinct columns: {}", task.getDistinctColumns());
        }

        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        return new FilteredPageOutput(task, inputSchema,
                                      outputSchema, output);
    }

    private List<Column> convertNameToColumn(Schema inputSchema, List<String> columnNames)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (String columnName : columnNames) {
            for (Column column : inputSchema.getColumns()) {
                if (columnName.contentEquals(column.getName())) {
                    builder.add(column);
                }
            }
        }

        return builder.build();
    }
}
