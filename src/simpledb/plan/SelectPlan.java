package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.SelectScan;
import simpledb.record.Schema;

public class SelectPlan implements Plan {

    private Plan p;
    private Predicate pred;

    public SelectPlan(Plan p, Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    @Override
    public Scan open() {
        Scan sc = p.open();
        return new SelectScan(sc, pred);
    }

    @Override
    public int estimatedNumBlocks() {
        return p.estimatedNumBlocks();
    }

    @Override
    public int estimatedNumRecordsOutput() {
        return p.estimatedNumRecordsOutput() / pred.reductionFactor(p);
    }

    @Override
    public int estimatedNumDistinctValues(String fldName) {
        if (pred.equatesWithConstant(fldName) != null)
            return 1;
        else {
            String fldName2 = pred.equatesWithField(fldName);
            if (fldName2 != null)
                return Math.min(p.estimatedNumDistinctValues(fldName), p.estimatedNumDistinctValues(fldName2));
            else
                return p.estimatedNumDistinctValues(fldName);
        }
    }

    @Override
    public Schema getSchema() {
        return p.getSchema();
    }
}
