package org.embulk.filter.distinct;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;

/**
 * Created by takahiro.nakayama on 12/6/15.
 */
class ColumnVisitorImpl
    implements ColumnVisitor
{
    private final static Logger logger = Exec.getLogger(ColumnVisitorImpl.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;

    ColumnVisitorImpl(PageReader pageReader, PageBuilder pageBuilder) {
        this.pageReader = pageReader;
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
    public void jsonColumn(Column column) {
        throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
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
