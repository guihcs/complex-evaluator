package evaluator;

import dataset.DatasetManager;
import fr.inrialpes.exmo.align.impl.BasicAlignment;
import fr.inrialpes.exmo.align.impl.edoal.EDOALAlignment;
import fr.inrialpes.exmo.align.impl.renderer.RDFRendererVisitor;
import fr.inrialpes.exmo.align.parser.AlignmentParser;
import org.apache.jena.rdf.model.RDFNode;
import org.semanticweb.owl.align.Alignment;
import org.semanticweb.owl.align.AlignmentException;
import org.semanticweb.owl.align.AlignmentVisitor;
import sparql.SparqlProxy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class Evaluator {

    /**
     * @param args args[0] = source ontology name
     *             args[1] = target ontology name
     *             args[2] = source ontology path
     *             args[3] = target ontology path
     *             args[4] = matcher output folder
     *             args[5] = CQA folder
     *             args[6] = result folder
     */
    public static void main(String[] args) throws IOException {

        System.out.println("Evaluator");
        String sourceOnto = args[0];
        String targetOnto = args[1];

        DatasetManager.getInstance().load(sourceOnto, args[2]);
        DatasetManager.getInstance().load(targetOnto, args[3]);

        String resultFolder = args[4];
        String queriesPath = args[5];
        String resultFile = args[6];

        Files.createDirectories(Paths.get(resultFile + "/CQA_coverage"));
        PrintWriter cvWriter = new PrintWriter(resultFile + "/CQA_coverage/" + sourceOnto + "_" + targetOnto + ".csv", StandardCharsets.UTF_8);
        PrintWriter writer = new PrintWriter(resultFile + "/" + sourceOnto + "_" + targetOnto + ".csv", StandardCharsets.UTF_8);
        AtomicInteger count = new AtomicInteger();
        float[] means = new float[5];
        try (Stream<Path> walk = Files.walk(Paths.get(resultFolder), 1)) {
            walk.forEach(fpath -> {
                if (Files.isDirectory(fpath)) return;
                AlignmentParser ap = new AlignmentParser();
                new BasicAlignment();
                BasicAlignment al;

                List<Set<String>> sourceResults = new ArrayList<>();
                List<String> targetQueries = new ArrayList<>();
                List<String> sourceQueries;

                File alignmentfile = fpath.toFile();

                try {
                    if (ap.parse(alignmentfile.toURI()) instanceof EDOALAlignment edoal) {
                        al = (BasicAlignment) ap.parse(alignmentfile.toURI());

                        File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto + "-" + targetOnto + ".edoal", targetOnto + "-" + sourceOnto + ".edoal"));
                        ensureInverseAlignmentFile(al, inverseAlignmentFile);

                        sourceQueries = edoal.toSourceSPARQLQuery();
                        targetQueries = edoal.toTargetSPARQLQuery();

                        for (int i = 0; i < targetQueries.size(); i++) {
                            sourceResults.add(getSPARQLQueryResults(sourceOnto, sourceQueries.get(i)));

                        }

                    } else {
                        al = (BasicAlignment) ap.parse(alignmentfile.toURI());

                        File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto + "-" + targetOnto + ".rdf", targetOnto + "-" + sourceOnto + ".rdf"));
                        ensureInverseAlignmentFile(al, inverseAlignmentFile);

                    }
                } catch (AlignmentException | FileNotFoundException e) {
                    throw new RuntimeException(e);
                }


                StringBuilder resultCSV = new StringBuilder("cqa,best_q_prec,best_q_fmeasure,best_q_rec\n");

                List<Double> precisions = new ArrayList<>();
                List<Double> recalls = new ArrayList<>();
                List<Double> fMeasures = new ArrayList<>();

                AtomicReference<List<String>> rewrittenQueries = new AtomicReference<>(new ArrayList<>());
                BasicAlignment finalAl = al;
                List<String> finalTargetQueries = targetQueries;
                try (Stream<Path> files = Files.walk(Paths.get(queriesPath), 1)) {
                    files.forEach(path -> {
                        try {
                            evaluate(sourceOnto, targetOnto, precisions, recalls, fMeasures, sourceResults, rewrittenQueries, resultCSV, finalAl, finalTargetQueries, path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                resultCSV.append("global mean,").append(mean(precisions)).append(",").append(mean(fMeasures)).append(",").append(mean(recalls));


                String proportionQueries = calcResults(precisions, recalls, fMeasures);


                cvWriter.println(fpath.getFileName().toString() + "," + resultCSV);
                writer.println(fpath.getFileName().toString() + "," + proportionQueries);

                String[] split = proportionQueries.split(",");

                for (int i = 1; i < split.length; i++) {
                    means[i - 1] += Float.parseFloat(split[i]);
                }
                count.getAndIncrement();
            });
        }
        writer.println("classical,recall-oriented,precision-oriented,overlap,query f-measure");
        writer.printf("MEAN,CQAs,%f,%f,%f,%f,%f\n", means[0] / count.get(), means[1] / count.get(), means[2] / count.get(), means[3] / count.get(), means[4] / count.get());
        cvWriter.close();
        writer.close();

        DatasetManager.getInstance().close();

    }

    private static void ensureInverseAlignmentFile(Alignment al, File inverseAlignmentFile) throws AlignmentException, FileNotFoundException {
        if (inverseAlignmentFile.exists()) {
            return;
        }
        Alignment inverseAl = al.inverse();
        PrintWriter writer = new PrintWriter(inverseAlignmentFile);
        AlignmentVisitor renderer = new RDFRendererVisitor(writer);
        inverseAl.render(renderer);
        writer.flush();
        writer.close();
    }

    private static void evaluate(String sourceOnto, String targetOnto, List<Double> precisions, List<Double> recalls, List<Double> fMeasures, List<Set<String>> sourceResults, AtomicReference<List<String>> rewrittenQueries, StringBuilder resultCSV, BasicAlignment finalAl, List<String> finalTargetQueries, Path path) throws IOException {
        String cqa = path.getFileName().toString();
        Path sourceCQAFile = Paths.get(path + "/" + sourceOnto + ".sparql");
        Path targetCQAFile = Paths.get(path + "/" + targetOnto + ".sparql");

        if (Files.notExists(sourceCQAFile) || Files.notExists(targetCQAFile)) {
            return;
        }

        String sourceCQA = getQueryContent(sourceCQAFile);
        String targetCQA = getQueryContent(targetCQAFile);
        Set<String> sourceCQAresults = getSPARQLQueryResults(sourceOnto, sourceCQA);
        Set<String> targetCQAresults = getSPARQLQueryResults(targetOnto, targetCQA);

        if (finalAl instanceof EDOALAlignment edoalAlignment) {
            rewrittenQueries.set(edoalAlignment.rewriteAllPossibilitiesQuery(sourceCQA));
            for (int j = 0; j < sourceResults.size(); j++) {
                if (!identical(compareSet(sourceResults.get(j), sourceCQAresults))) {
                    continue;
                }
                rewrittenQueries.get().add(finalTargetQueries.get(j));
            }
        } else {
            try {
                rewrittenQueries.get().add(finalAl.rewriteSPARQLQuery(sourceCQA));
            } catch (AlignmentException e) {
                throw new RuntimeException(e);
            }
        }

        List<Double> bestRes = new ArrayList<>();
        bestRes.add(0.0);
        bestRes.add(0.0);
        AtomicReference<Double> bestFmeasure = new AtomicReference<>(0.0);

        try(ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            for (String rewrittenQuery : rewrittenQueries.get()) {
                executor.submit(() -> {

                    Set<String> rewrittenResults = getSPARQLQueryResults(targetOnto, rewrittenQuery);
                    List<Double> rewrittenRes = compareSet(targetCQAresults, rewrittenResults);

                    if (fMeasure(rewrittenRes) > bestFmeasure.get()) {
                        bestRes.set(0, rewrittenRes.get(0));
                        bestRes.set(1, rewrittenRes.get(1));
                        bestFmeasure.set(fMeasure(rewrittenRes));
                    }
                });
            }
        }


        precisions.add(bestRes.get(0));
        recalls.add(bestRes.get(1));
        fMeasures.add(bestFmeasure.get());
        resultCSV.append(cqa).append(",").append(bestRes.get(0)).append(",").append(bestFmeasure.get()).append(",").append(bestRes.get(1)).append("\n");
    }

    public static Set<String> getSPARQLQueryResults(String onto, String query) {
        Set<String> results = new HashSet<>();
        int offset = 0;
        int limit = 10000;
        boolean end = false;
        while (!end) {
            String newQuery = query;
            newQuery += "\n LIMIT " + limit;
            newQuery += "\n OFFSET " + offset;

            List<Map<String, RDFNode>> result = SparqlProxy.query(onto, newQuery);
            Iterator<Map<String, RDFNode>> retIterator = result.iterator();
            int nbAns = 0;
            while (retIterator.hasNext()) {
                nbAns++;
                Map<String, RDFNode> ans = retIterator.next();
                if (ans.containsKey("s") && ans.get("s") != null) {
                    String s = instanceString(ans.get("s").toString());
                    String o = "";
                    if (ans.containsKey("o") && ans.get("o") != null) {
                        o = instanceString(ans.get("o").toString());
                    }
                    results.add(s + o);
                }
            }
            if (nbAns < limit) {
                end = true;
            } else {
                offset += limit;
            }
            if (offset > 60000) {
                end = true;
            }
        }
        return results;
    }

    private static String calcResults(List<Double> precisions, List<Double> recalls, List<Double> fMeasures) {
        double nbEquivCQA = 0;
        double nbMoreGeneralCQA = 0;
        double nbMoreSpecificCQA = 0;
        double nbOverlapCQA = 0;
        for (int i = 0; i < precisions.size(); i++) {
            List<Double> result = new ArrayList<>();
            result.add(precisions.get(i));
            result.add(recalls.get(i));
            if (identical(result)) {
                nbEquivCQA++;
            } else if (sourceMoreSpecificThanTarget(result)) {
                nbMoreGeneralCQA++;
            } else if (sourceMoreGeneralThanTarget(result)) {
                nbMoreSpecificCQA++;
            } else if (overlap(result)) {
                nbOverlapCQA++;
            }
        }

        return "CQAs," + nbEquivCQA / precisions.size() + "," + (nbEquivCQA + nbMoreGeneralCQA + 0.5 * nbMoreSpecificCQA) / precisions.size() +
                "," + (nbMoreSpecificCQA + nbEquivCQA + 0.5 * nbMoreGeneralCQA) / precisions.size() + "," +
                (nbEquivCQA + nbOverlapCQA + nbMoreGeneralCQA + nbMoreSpecificCQA) / precisions.size() + ","
                + mean(fMeasures);
    }


    public static boolean sourceMoreGeneralThanTarget(List<Double> results) {
        return (results.get(0) == 1 && results.get(1) > 0);
    }

    public static boolean sourceMoreSpecificThanTarget(List<Double> results) {
        return (results.get(0) > 0 && results.get(1) == 1);
    }


    public static List<Double> compareSet(Set<String> source, Set<String> target) {
        List<Double> results = new ArrayList<>();
        int correctResults = 0;
        for (String targRes : target) {
            if (source.contains(targRes)) {
                correctResults++;
            }
        }
        double prec = 0;
        double rec = 0;
        if (!source.isEmpty() && !target.isEmpty()) {
            prec = (double) correctResults / (double) target.size();
            rec = (double) correctResults / (double) source.size();
        }
        results.add(prec);
        results.add(rec);
        return results;
    }

    public static double mean(List<Double> list) {
        double res = 0;
        for (double d : list) {
            res += d;
        }
        return res / (double) list.size();
    }

    public static boolean identical(List<Double> results) {
        return results.get(0) == 1 && results.get(1) == 1;
    }

    public static boolean overlap(List<Double> results) {
        return results.get(0) > 0 && results.get(1) > 0;
    }

    public static double fMeasure(List<Double> results) {
        double prec = results.get(0);
        double rec = results.get(1);
        if (prec == 0 && rec == 0) {
            return 0;
        } else {
            return 2 * (prec * rec) / (prec + rec);
        }
    }



    public static String getQueryContent(Path f) throws IOException {
        StringBuilder result = new StringBuilder();
        Scanner sc = new Scanner(Files.newBufferedReader(f));

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            result.append(line).append(" ");
        }
        sc.close();
        return result.toString();
    }


    public static String instanceString(String raw) {

        return raw
                .replaceAll("\"", "")
                .replaceAll("http://[^#]+#", "")
                .replaceAll("_v0", "")
                .replaceAll("_v2", "");
    }

}
