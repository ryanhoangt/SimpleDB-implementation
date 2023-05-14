package simpledb.parse;

public class PredParser {

    private Lexer lex;

    public PredParser(String s) {
        lex = new Lexer(s);
    }

    // <Field := IdTok
    public void field() {
        lex.eatId();
    }

    // <Constant> := StrTok | IntTok
    public void constant() {
        if (lex.matchStringConstant())
            lex.eatStringConstant();
        else
            lex.eatIntConstant();
    }

    // <Expression> := <Field> | <Constant>
    public void expression() {
        if (lex.matchId())
            field();
        else
            constant();
    }

    // <Term> := <Expression> = <Expression>
    public void term() {
        expression();
        lex.eatDelim('=');
        expression();
    }

    // <Predicate> := <Term> [ AND <Predicate> ]
    public void predicate() {
        term();
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            predicate();
        }
    }
}
