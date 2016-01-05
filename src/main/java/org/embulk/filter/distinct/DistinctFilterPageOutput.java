package org.embulk.filter.distinct;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.embulk.filter.distinct.DistinctFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * Created by takahiro.nakayama on 12/6/15.
 */
class DistinctFilterPageOutput
    implements PageOutput
{
    private final static Logger logger = Exec.getLogger(DistinctFilterPageOutput.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final ColumnVisitorImpl visitor;
    private final Schema outputSchema;
    private final List<Column> distinctColumns;

    private final static Set<List<Object>> set = Sets.newConcurrentHashSet();

    DistinctFilterPageOutput(PluginTask task, Schema inputSchema,
                             Schema outputSchema, PageOutput pageOutput)
    {
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.visitor = new ColumnVisitorImpl(pageReader, pageBuilder);
        this.outputSchema = outputSchema;
        this.distinctColumns = task.getDistinctColumns();
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            if (isDistinct(getCurrentValues())) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageBuilder.close();
    }

    private List<Object> getCurrentValues()
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (Column distinctColumn : distinctColumns) {
            if (pageReader.isNull(distinctColumn)) {
                builder.add(Optional.absent());
            }
            else if (Types.BOOLEAN.equals(distinctColumn.getType())) {
                builder.add(pageReader.getBoolean(distinctColumn));
            }
            else if (Types.DOUBLE.equals(distinctColumn.getType())) {
                builder.add(pageReader.getDouble(distinctColumn));
            }
            else if (Types.LONG.equals(distinctColumn.getType())) {
                builder.add(pageReader.getLong(distinctColumn));
            }
            else if (Types.STRING.equals(distinctColumn.getType())) {
                builder.add(pageReader.getString(distinctColumn));
            }
            else if (Types.TIMESTAMP.equals(distinctColumn.getType())) {
                builder.add(pageReader.getTimestamp(distinctColumn));
            }
            else {
                throw new RuntimeException("unsupported type: " + distinctColumn.getType());
            }
        }

        return builder.build();
    }

    private boolean isDistinct(List<Object> values) {
        if (set.add(values)) {
            return true;
        }
        else {
            logger.debug("Duplicated values: {}", values);
            return false;
        }
    }
}
