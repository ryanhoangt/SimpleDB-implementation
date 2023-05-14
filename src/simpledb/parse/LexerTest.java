package simpledb.parse;

import java.util.Scanner;

public class LexerTest {
    public static void main(String[] args) {
        String x = "";
        int y = 0;

        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String s = sc.nextLine();
            Lexer lex = new Lexer(s);
            if (lex.matchId()) {
                x = lex.eatId();
                lex.eatDelim('=');
                y = lex.eatIntConstant();
            } else {
                y = lex.eatIntConstant();
                lex.eatDelim('=');
                x = lex.eatId();
            }
            System.out.println(x + " equals " + y);
        }
        sc.close();
    }
}
