package simpledb.parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Scanner;

public class TokenizerTest {

    private static Collection<String> KEYWORDS = Arrays.asList(
            "select", "from", "where", "and", "insert", "into",
            "values", "delete", "update", "set", "create", "table",
            "int", "varchar", "view", "as", "index", "on"
    );

    public static void main(String[] args) throws IOException {
        String s = getStringFromUser();
        StreamTokenizer tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.');
        tok.lowerCaseMode(true); // ids and keywords are converted to lowercase

        while (tok.nextToken() != StreamTokenizer.TT_EOF) {
            printCurrentToken(tok);
        }
    }

    private static String getStringFromUser() {
        System.out.println("Enter tokens: ");
        Scanner sc = new Scanner(System.in);
        String s = sc.nextLine();
        sc.close();
        return s;
    }

    private static void printCurrentToken(StreamTokenizer tok) {
        if (tok.ttype == StreamTokenizer.TT_NUMBER)
            System.out.println("IntConstant " + (int) tok.nval);
        else if (tok.ttype == StreamTokenizer.TT_WORD) {
            String word = tok.sval;
            if (KEYWORDS.contains(word))
                System.out.println("Keyword " + word);
            else
                System.out.println("Id " + word);
        }
        else if (tok.ttype == '\'') // single quote
            System.out.println("StringConstant " + tok.sval);
        else
            System.out.println("Delimiter " + (char) tok.ttype);
    }
}
