package simpledb.record;

import simpledb.file.Page;

import java.util.HashMap;
import java.util.Map;
import java.sql.Types;

public class Layout {

    private Schema schema;
    private Map<String, Integer> fieldToOffset;
    private int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;

        this.fieldToOffset = new HashMap<>();
        int pos = Integer.BYTES; // space for empty/in-use flag
        for (String fieldName: schema.fields()) {
            fieldToOffset.put(fieldName, pos);
            pos += lengthInBytes(fieldName);
        }
        this.slotSize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> fieldToOffset, int slotSize) {
        this.schema = schema;
        this.fieldToOffset = fieldToOffset;
        this.slotSize = slotSize;
    }

    public int offsetOfField(String fieldName) {
        return fieldToOffset.get(fieldName);
    }

    public Schema getSchema() {
        return schema;
    }

    public int getSlotSize() {
        return slotSize;
    }

    private int lengthInBytes(String fieldName) {
        int fieldType = schema.type(fieldName);
        if (fieldType == Types.INTEGER)
            return Integer.BYTES;
        else
            return Page.maxLength(schema.length(fieldName));
    }
}
