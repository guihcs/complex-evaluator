package t1;

import fr.inrialpes.exmo.align.parser.AlignmentParser;
import org.junit.jupiter.api.Test;
import org.semanticweb.owl.align.Alignment;
import org.semanticweb.owl.align.AlignmentException;

import java.nio.file.Paths;

public class T1Test {



    @Test
    public void test1() throws AlignmentException {
        String path = "/home/guilherme/Downloads/edas-conference.edoal";

        AlignmentParser ap = new AlignmentParser();

        Alignment parse = ap.parse(Paths.get(path).toUri());

    }
}
