package simpledb.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.sql.Types;

public class Schema {

    class FieldInfo {
        int type, length;

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }

    private List<String> fields = new ArrayList<>();
    private Map<String, FieldInfo> fieldsInfo = new HashMap<>();

    /**
     * Add a field to the schema
     * @param fieldName name of the field
     * @param type java.sql.Types.INTEGER or VARCHAR
     * @param length conceptual length of String field i.e. max number of chars
     */
    public void addField(String fieldName, int type, int length) {
        fields.add(fieldName);
        fieldsInfo.put(fieldName, new FieldInfo(type, length));
    }

    public void addIntField(String fieldName) {
        addField(fieldName, Types.INTEGER, 0);
    }

    public void addStringField(String fieldName, int length) {
        addField(fieldName, Types.VARCHAR, length);
    }

    /**
     * Add a field with name from a given other Schema to this Schema.
     * @param fieldName field name to add
     * @param schema another Schema object
     */
    public void add(String fieldName, Schema schema) {
        int type = schema.type(fieldName);
        int length = schema.length(fieldName);
        addField(fieldName, type, length);
    }

    /**
     * Add all fields from a given other Schema to this Schema
     * @param schema another Schema object
     */
    public void addAll(Schema schema) {
        for (String fieldName: schema.fields()) {
            add(fieldName, schema);
        }
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public int type(String fieldName) {
        return fieldsInfo.get(fieldName).type;
    }

    public int length(String fieldName) {
        return fieldsInfo.get(fieldName).length;
    }
}
