package simpledb.parse;

import simpledb.query.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {

    private List<String> fields;
    private Collection<String> tables;
    private Predicate pred;

    public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
        this.fields = fields;
        this.tables = tables;
        this.pred = pred;
    }

    public List<String> getFields() {
        return fields;
    }

    public Collection<String> getTables() {
        return tables;
    }

    public Predicate getPred() {
        return pred;
    }

    public String toString() {
        String result = "select ";
        for (String fldName: fields)
            result += fldName + ", ";
        result = result.substring(0, result.length()-2); // remove final comma
        result += " from ";
        for (String tblName: tables)
            result += tblName + ", ";
        result = result.substring(0, result.length()-2);
        String predStr = pred.toString();
        if (!predStr.equals(""))
            result += " where " + predStr;
        return result;
    }
}
