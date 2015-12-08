package org.embulk.filter.distinct;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
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
class FilteredPageOutput
    implements PageOutput
{
    private final static Logger logger = Exec.getLogger(FilteredPageOutput.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final ColumnVisitorImpl visitor;
    private final Schema outputSchema;
    private final List<Column> distinctColumns;
    private final String delimiter;

    private final static Set<List<Object>> filter = Sets.newConcurrentHashSet();

    FilteredPageOutput(PluginTask task, Schema inputSchema,
                       Schema outputSchema, PageOutput pageOutput)
    {
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.visitor = new ColumnVisitorImpl(pageReader, pageBuilder);
        this.outputSchema = outputSchema;
        this.distinctColumns = task.getDistinctColumns();
        this.delimiter = task.getDelimiter();
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            if (filter.add(getCurrentDistinctKey())) {
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

//    private String getCurrentDistinctKey()
    private List<Object> getCurrentDistinctKey()
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
//        StringBuilder sb = new StringBuilder();
        for (Column distinctColumn : distinctColumns) {
            if (!pageReader.isNull(distinctColumn)) {
                if (Types.BOOLEAN.equals(distinctColumn.getType())) {
//                    sb.append(pageReader.getBoolean(distinctColumn));
                    builder.add(pageReader.getBoolean(distinctColumn));
                }
                else if (Types.DOUBLE.equals(distinctColumn.getType())) {
//                    sb.append(pageReader.getDouble(distinctColumn));
                    builder.add(pageReader.getDouble(distinctColumn));
                }
                else if (Types.LONG.equals(distinctColumn.getType())) {
//                    sb.append(pageReader.getLong(distinctColumn));
                    builder.add(pageReader.getLong(distinctColumn));
                }
                else if (Types.STRING.equals(distinctColumn.getType())) {
//                    sb.append(pageReader.getString(distinctColumn));
                    builder.add(pageReader.getString(distinctColumn));
                }
                else if (Types.TIMESTAMP.equals(distinctColumn.getType())) {
//                    sb.append(pageReader.getTimestamp(distinctColumn));
                    builder.add(pageReader.getTimestamp(distinctColumn));
                }
                else {
                    throw new RuntimeException("unsupported type: " + distinctColumn.getType());
                }
            }
//            sb.append(delimiter);
        }

//        return sb.toString();
        return builder.build();
    }
}
