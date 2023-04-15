package simpledb.query;

import simpledb.record.Schema;

/**
 * Represent a SQL expression, can be either a Constant
 * or a field name of a table.
 */
public class Expression {

    private Constant val = null;
    private String fldName = null;

    public Expression(Constant val) {
        this.val = val;
    }

    public Expression(String fldName) {
        this.fldName = fldName;
    }

    public boolean isFieldName() {
        return fldName != null;
    }

    public Constant asConstant() {
        return val;
    }

    public String asFieldName() {
        return fldName;
    }

    /**
     * Evaluate the expression w.r.t the current record of the
     * specified scan.
     */
    public Constant evaluate(Scan sc) {
        return (val != null) ? val : sc.getVal(fldName);
    }

    /**
     * Determine whether the field in the expression (if any)
     * is contained in the specified schema.
     */
    public boolean appliesTo(Schema schema) {
        return val != null || schema.hasField(fldName);
    }

    @Override
    public String toString() {
        return (val != null) ? val.toString() : fldName;
    }
}
