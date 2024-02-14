package evaluator;

import dataset.DatasetManager;
import fr.inrialpes.exmo.align.impl.BasicAlignment;
import fr.inrialpes.exmo.align.impl.edoal.EDOALAlignment;
import fr.inrialpes.exmo.align.impl.renderer.RDFRendererVisitor;
import fr.inrialpes.exmo.align.parser.AlignmentParser;
import org.semanticweb.owl.align.AlignmentException;
import org.semanticweb.owl.align.AlignmentVisitor;
import org.semanticweb.owl.align.Cell;
import sparql.SparqlProxy;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class Precision {

    /**
     * @param args args[0] = source name
     *             args[1] = target name
     *             args[2] = alignment folder
     *             args[3] = source ontology path
     *             args[4] = target ontology path
     *             args[5] = results folder
     */
    public static void main(String[] args) throws IOException {


        String baseFolder = args[2];


        AtomicReference<ArrayList<String>> targetQueries = new AtomicReference<>(new ArrayList<>());
        AtomicReference<ArrayList<String>> sourceQueries = new AtomicReference<>(new ArrayList<>());
        String sourceOnto = args[0];
        String targetOnto = args[1];

        DatasetManager.getInstance().load(sourceOnto, args[3]);
        DatasetManager.getInstance().load(targetOnto, args[4]);

        PrintWriter finalWriter = new PrintWriter(args[5] + "/prec-" + sourceOnto + "-" + targetOnto + ".csv", StandardCharsets.UTF_8);
        finalWriter.println("classical,recall-oriented,precision-oriented,overlap,query f-measure");


        try(Stream<Path> walk = Files.walk(Paths.get(baseFolder), 1)) {
            walk.forEach(fpath -> {
                if (Files.isDirectory(fpath)) return;
                AlignmentParser ap = new AlignmentParser();
                new BasicAlignment();
                BasicAlignment al;

                File alignmentfile = fpath.toFile();


                try {
                    if (ap.parse(alignmentfile.toURI()) instanceof EDOALAlignment) {
                        al = (BasicAlignment) ap.parse(alignmentfile.toURI());
                        File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto + "-" + targetOnto + ".edoal", targetOnto + "-" + sourceOnto + ".edoal"));
                        if (!inverseAlignmentFile.exists()) {
                            EDOALAlignment inverseAl = (EDOALAlignment) al.inverse();
                            PrintWriter writer = new PrintWriter(inverseAlignmentFile);
                            AlignmentVisitor renderer = new RDFRendererVisitor(writer);
                            inverseAl.render(renderer);
                            writer.flush();
                            writer.close();
                        }
                        targetQueries.set(((EDOALAlignment) al).toTargetSPARQLQuery());
                        sourceQueries.set(((EDOALAlignment) al).toSourceSPARQLQuery());
                    } else {
                        al = (BasicAlignment) ap.parse(alignmentfile.toURI());
                        File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto + "-" + targetOnto + ".rdf", targetOnto + "-" + sourceOnto + ".rdf"));
                        if (!inverseAlignmentFile.exists()) {
                            BasicAlignment inverseAl = (BasicAlignment) al.inverse();
                            PrintWriter writer = new PrintWriter(inverseAlignmentFile);
                            AlignmentVisitor renderer = new RDFRendererVisitor(writer);
                            inverseAl.render(renderer);
                            writer.flush();
                            writer.close();
                        }
                        for (Cell cell : al) {
                            URI uri1 = cell.getObject1AsURI(al);
                            URI uri2 = cell.getObject2AsURI(al);

                            if (SparqlProxy.sendAskQuery(sourceOnto, "ASK{ ?x a <" + uri1 + ">}") || SparqlProxy.sendAskQuery(targetOnto, "ASK{ ?x a <" + uri2 + ">}")) {
                                sourceQueries.get().add("SELECT DISTINCT ?s WHERE { ?s a <" + uri1 + ">.}");
                                targetQueries.get().add("SELECT DISTINCT ?s WHERE { ?s a <" + uri2 + ">.}");
                            } else {
                                sourceQueries.get().add("SELECT DISTINCT ?s ?o WHERE { ?s <" + uri1 + "> ?o.}");
                                targetQueries.get().add("SELECT DISTINCT ?s ?o WHERE { ?s <" + uri2 + "> ?o.}");
                            }
                        }
                    }
                } catch (AlignmentException | FileNotFoundException e) {
                    throw new RuntimeException(e);
                }


                double numEquiv = 0;
                double nbMoreGeneral = 0;
                double nbMoreSpecific = 0;
                double numOverlap = 0;
                double numDisjoint = 0;
                double numEmpty = 0;
                List<Double> fmeasures = new ArrayList<>();

                List<Set<String>> sourceResults = new ArrayList<>();
                List<Set<String>> targetResults = new ArrayList<>();
                for (int i = 0; i < targetQueries.get().size(); i++) {
                    sourceResults.add(Evaluator.getSPARQLQueryResults(sourceOnto, sourceQueries.get().get(i)));
                    targetResults.add(Evaluator.getSPARQLQueryResults(targetOnto, targetQueries.get().get(i)));
                    List<Double> comp = Evaluator.compareSet(sourceResults.get(i), targetResults.get(i));

                    fmeasures.add(Evaluator.fMeasure(comp));
                    if (sourceResults.get(i).isEmpty() && targetResults.get(i).isEmpty()) {
                        numEmpty++;
                    } else {
                        if (Evaluator.identical(comp)) {
                            numEquiv++;
                        } else if (sourceMoreSpecificThanTarget(comp)) {
                            nbMoreGeneral++;
                        } else if (sourceMoreGeneralThanTarget(comp)) {
                            nbMoreSpecific++;
                        } else if (Evaluator.overlap(comp)) {
                            numOverlap++;
                        } else if (disjoint(comp)) {
                            numDisjoint++;
                        }
                    }
                }
                String resultCSV = numEquiv / sourceResults.size() + "," +
                        (numEquiv + nbMoreGeneral + 0.5 * nbMoreSpecific) / sourceResults.size() + "," +
                        (numEquiv + 0.5 * nbMoreGeneral + nbMoreSpecific) / sourceResults.size() + "," +
                        (numEquiv + nbMoreGeneral + nbMoreSpecific + numOverlap) / sourceResults.size() + "," +
                        Evaluator.mean(fmeasures) + "," + (numEquiv + nbMoreGeneral + nbMoreSpecific + numOverlap + numEmpty) / sourceResults.size();


                finalWriter.println(fpath.getFileName().toString() + "," + resultCSV);


            });

        }
        finalWriter.close();
    }


    public static boolean disjoint(List<Double> results) {
        return results.get(0) == 0 && results.get(1) == 0;
    }

    public static boolean sourceMoreGeneralThanTarget(List<Double> results) {
        return (results.get(0) == 1 && results.get(1) > 0);
    }

    public static boolean sourceMoreSpecificThanTarget(List<Double> results) {
        return (results.get(0) > 0 && results.get(1) == 1);
    }

}
