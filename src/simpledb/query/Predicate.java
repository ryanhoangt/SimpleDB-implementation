package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A predicate is a boolean combination of Terms.
 */
public class Predicate {

    private List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    public Predicate(Term term) {
        terms.add(term);
    }

    /**
     * Returns true if the predicate evaluates to true w.r.t
     * the specified scan.
     * @param sc the Scan object
     * @return true if the predicate is true in the scan
     */
    public boolean isSatisfied(Scan sc) {
        for (Term term: terms) {
            if (!term.isSatisfied(sc))
                return false;
        }
        return true;
    }

    /**
     * Modifies the predicate to be the conjunction of itself
     * and the specified predicate.
     * @param pred the other predicate
     */
    public void conjoinWith(Predicate pred) {
        terms.addAll(pred.terms);
    }

    /**
     * Return the subpredicate that applies to the specified schema.
     * @param schema the schema
     * @return an Optional of the subpredicate
     */
    public Optional<Predicate> selectSubPredApplyingToSchema(Schema schema) {
        Predicate res = new Predicate();
        for (Term term: terms) {
            if (term.appliesTo(schema))
                res.terms.add(term);
        }
        if (res.terms.size() == 0)
            return Optional.empty();
        else
            return Optional.of(res);
    }

    /**
     * Return the subpredicate consisting of terms applying to the union
     * of the two specified schemas, but not to either schema separately.
     * e.g. sch1: id1, address, gender
     *      sch2: id2, type, size
     *      newSch: id1, id2, address, gender, type, size
     *      term: id1=id2
     * @param sch1 the first schema
     * @param sch2 the second schema
     * @return an Optional of the subpredicate whose terms apply to the union of
     * the two schemas but not either schema separately.
     */
    public Optional<Predicate> selectSubPredApplyingToUnionOf(Schema sch1, Schema sch2) {
        Predicate res = new Predicate();
        Schema newSch = new Schema();
        newSch.addAll(sch1);
        newSch.addAll(sch2);
        for (Term term: terms) {
            if (!term.appliesTo(sch1) && !term.appliesTo(sch2) && term.appliesTo(newSch))
                res.terms.add(term);
        }
        if (res.terms.size() == 0)
            return Optional.empty();
        else
            return Optional.of(res);
    }

    /**
    * Determine if there is a term of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldName the name of the field
    * @return either the constant or null
    */
    public Constant equatesWithConstant(String fldName) {
        for (Term t: terms) {
            Constant c = t.equatesWithConstant(fldName);
            if (c != null)
                return c;
        }
        return null;
    }

    /**
     * Determine if there is a term of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fldName the name of the field
     * @return the name of the other field, or null
     */
    public String equatesWithField(String fldName) {
        for (Term t: terms) {
            String s = t.equatesWithField(fldName);
            if (s != null) return s;
        }
        return null;
    }

    /**
     * Calculate the extent to which selecting on the predicate
     * reduces the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * predicate cuts the size of the output in half.
     * @param plan the query's plan
     * @return the integer reduction factor.
     */
    public int reductionFactor(Plan plan) {
        int factor = 1;
        for (Term t: terms) {
            factor *= t.reductionFactor(plan);
        }
        return factor;
    }

    @Override
    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext())
            return "";
        StringBuilder res = new StringBuilder(iter.next().toString());
        while (iter.hasNext())
            res.append(" and ").append(iter.next().toString());
        return res.toString();
    }
}
