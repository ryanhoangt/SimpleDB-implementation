package simpledb.parse;

import simpledb.record.Schema;

public class CreateTableData {

    private String tblName;
    private Schema schema;

    public CreateTableData(String tblName, Schema schema) {
        this.tblName = tblName;
        this.schema = schema;
    }

    public String getTblName() {
        return tblName;
    }

    public Schema getSchema() {
        return schema;
    }
}
