package simpledb.query;

import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 */
public class Term {

    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    /**
     * Return true if both of the term's expressions evaluate to the same
     * constant.
     */
    public boolean isSatisfied(Scan sc) {
        Constant lhsVal = lhs.evaluate(sc);
        Constant rhsVal = rhs.evaluate(sc);
        return rhsVal.equals(lhsVal);
    }

    /**
     * Return true if both of the term's expressions apply to the specified
     * schema.
     */
    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    @Override
    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
