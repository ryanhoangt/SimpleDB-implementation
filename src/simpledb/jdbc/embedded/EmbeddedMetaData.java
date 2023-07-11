package simpledb.jdbc.embedded;

import simpledb.jdbc.ResultSetMetaDataAdapter;
import simpledb.record.Schema;

import java.sql.SQLException;

import static java.sql.Types.INTEGER;

/**
 * The embedded implementation of ResultSetMetaData.
 *
 * +-------------------+----------------+
 * | JDBC Interface    | SimpleDB Class |
 * +-------------------+----------------+
 * | ResultSetMetaData | Schema         |
 * +-------------------+----------------+
 */
public class EmbeddedMetaData extends ResultSetMetaDataAdapter {

    private Schema schema;

    /**
     * Creates a metadata object that wraps the specified schema.
     * The method also creates a list to hold the schema's
     * collection of field names,
     * so that the fields can be accessed by position.
     * @param schema the schema
     */
    public EmbeddedMetaData(Schema schema) {
        this.schema = schema;
    }

    /**
     * Returns the size of the field list.
     */
    @Override
    public int getColumnCount() throws SQLException {
        return schema.fields().size();
    }

    /**
     * Returns the number of characters required to display the
     * specified column.
     * For a string-type field, the method simply looks up the
     * field's length in the schema and returns that.
     * For an int-type field, the method needs to decide how
     * large integers can be.
     * Here, the method arbitrarily chooses 6 characters,
     * which means that integers over 999,999 will
     * probably get displayed improperly.
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        String fldName = getColumnName(column);
        int fldType = schema.type(fldName);
        int fldLen = (fldType == INTEGER) ? 6 : schema.length(fldName);
        return Math.max(fldName.length(), fldLen) + 1;
    }

    /**
     * Returns the field name for the specified column number.
     * In JDBC, column numbers start with 1, so the field
     * is taken from position (column-1) in the list.
     */
    @Override
    public String getColumnName(int column) throws SQLException {
        return schema.fields().get(column - 1);
    }

    /**
     * Returns the type of the specified column.
     * The method first finds the name of the field in that column,
     * and then looks up its type in the schema.
     */
    @Override
    public int getColumnType(int column) throws SQLException {
        String fldName = getColumnName(column);
        return schema.type(fldName);
    }
}
