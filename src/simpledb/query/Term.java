package simpledb.query;

import simpledb.plan.Plan;
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

    /**
     * Determine if this term is of the form "F=c" where F is the specified field
     * and c is some constant.
     * If so, returns that constant.
     * If not, returns null.
     * @param fldName the name of the field
     * @return either the constant or null
     */
    public Constant equatesWithConstant(String fldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fldName) && !rhs.isFieldName())
            return rhs.asConstant();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fldName) && !lhs.isFieldName())
            return lhs.asConstant();
        else
            return null;
    }

    /**
     * Determine if this term is of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fldName the name of the field
     * @return either the name of the other field, or null
     */
    public String equatesWithField(String fldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fldName) && rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fldName) && lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    /**
     * Calculate the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     * @param plan the query's plan
     * @return the reduction factor integer
     */
    public int reductionFactor(Plan plan) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(
                    plan.estimatedNumDistinctValues(lhsName),
                    plan.estimatedNumDistinctValues(rhsName)
            );
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return plan.estimatedNumDistinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return plan.estimatedNumDistinctValues(rhsName);
        }
        // otherwise, the term equates to constants
        if (lhs.asConstant().equals(rhs.asConstant()))
            return 1;
        else
            return Integer.MAX_VALUE;
    }

    @Override
    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
