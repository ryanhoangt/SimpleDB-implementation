package simpledb.plan;

import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class ProductPlan implements Plan {

    private Plan p1, p2;
    private Schema schema = new Schema();

    public ProductPlan(Plan p1, Plan p2) {
        this.p1 = p1;
        this.p2 = p2;
        schema.addAll(p1.getSchema());
        schema.addAll(p2.getSchema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new ProductScan(s1, s2);
    }

    @Override
    public int estimatedNumBlocks() {
        return p1.estimatedNumBlocks()
                + (p1.estimatedNumRecordsOutput() * p2.estimatedNumBlocks());
    }

    @Override
    public int estimatedNumRecordsOutput() {
        return p1.estimatedNumRecordsOutput() * p2.estimatedNumRecordsOutput();
    }

    @Override
    public int estimatedNumDistinctValues(String fldName) {
        if (p1.getSchema().hasField(fldName))
            return p1.estimatedNumDistinctValues(fldName);
        else
            return p2.estimatedNumDistinctValues(fldName);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
