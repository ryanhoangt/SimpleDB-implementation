package simpledb.parse;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private Lexer lex;

    public Parser(String s) {
        this.lex = new Lexer(s);
    }

    // Methods for parsing predicates and their components
    public String field() {
        return lex.eatId();
    }

    public Constant constant() {
        if (lex.matchStringConstant())
            return new Constant(lex.eatStringConstant());
        else
            return new Constant(lex.eatIntConstant());
    }

    public Expression expression() {
        if (lex.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public Term term() {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    // Methods for parsing queries
    public QueryData query() {
        lex.eatKeyword("select");
        List<String> fields = selectList();
        lex.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }

    private List<String> selectList() {
        List<String> l = new ArrayList<>();
        l.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            l.addAll(selectList());
        }
        return l;
    }

    private Collection<String> tableList() {
        Collection<String> l = new ArrayList<>();
        l.add(lex.eatId());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            l.addAll(tableList());
        }
        return l;
    }

    // Methods for parsing the various update commands
    public Object updateCmd() {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else if (lex.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else if (lex.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

    // Method for parsing delete commands
    public DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblName = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblName, pred);
    }

    // Methods for parsing insert commands
    public InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblName, flds, vals);
    }

    private List<String> fieldList() {
        List<String> l = new ArrayList<>();
        l.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            l.addAll(fieldList());
        }
        return l;
    }

    private List<Constant> constList() {
        List<Constant> l = new ArrayList<>();
        l.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            l.addAll(constList());
        }
        return l;
    }

    // Method for parsing
    public ModifyData modify() {
        lex.eatKeyword("update");
        String tblName = lex.eatId();
        lex.eatKeyword("set");
        String fldName = field();
        lex.eatDelim('=');
        Expression newVal = expression();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblName, fldName, newVal, pred);
    }

    // Method for parsing create table commands
    public CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblName, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldName = field();
        return fieldType(fldName);
    }

    private Schema fieldType(String fldName) {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldName);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldName, strLen);
        }
        return schema;
    }

    // Method for parsing create view commands
    public CreateViewData createView() {
        lex.eatKeyword("view");
        String viewName = lex.eatId();
        lex.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewName, qd);
    }

    // Method for parsing create index commands
    public CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxName = lex.eatId();
        lex.eatKeyword("on");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        String fldName = field();
        lex.eatDelim(')');
        return new CreateIndexData(idxName, tblName, fldName);
    }
}
