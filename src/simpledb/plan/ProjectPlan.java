package simpledb.plan;

import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {

    private Plan p;
    private Schema schema = new Schema();

    public ProjectPlan(Plan p, List<String> fieldList) {
        this.p = p;
        for (String fldName: fieldList)
            this.schema.add(fldName, p.getSchema());
    }

    @Override
    public Scan open() {
        Scan sc = p.open();
        return new ProjectScan(sc, schema.fields());
    }

    @Override
    public int estimatedNumBlocks() {
        return p.estimatedNumBlocks();
    }

    @Override
    public int estimatedNumRecordsOutput() {
        return p.estimatedNumRecordsOutput();
    }

    @Override
    public int estimatedNumDistinctValues(String fldName) {
        return p.estimatedNumDistinctValues(fldName);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
