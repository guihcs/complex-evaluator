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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class Evaluator {

    public static void main(String[] args) throws IOException {


        String sourceOnto = "edas";
        String targetOnto = "conference";

        DatasetManager.getInstance().load(sourceOnto, "/source_temp.ttl");
        DatasetManager.getInstance().load(targetOnto, "/target_temp.ttl");

        String resultFolder = "out";
        String queriesPath = "CQAs";
        String resultfile = "results1";

        Files.createDirectories(Paths.get(resultfile + "/CQA_coverage"));
        PrintWriter cvWriter = new PrintWriter(resultfile + "/CQA_coverage/" + sourceOnto + "_" + targetOnto + ".csv", StandardCharsets.UTF_8);
        PrintWriter writer = new PrintWriter(resultfile + "/" + sourceOnto + "_" + targetOnto + ".csv", StandardCharsets.UTF_8);
        AtomicInteger count = new AtomicInteger();
        float[] means = new float[5];
        try(Stream<Path> walk = Files.walk(Paths.get(resultFolder), 1)){
            walk.forEach(fpath -> {
                if (Files.isDirectory(fpath)) return;
                AlignmentParser ap = new AlignmentParser();
                BasicAlignment al = new BasicAlignment();

                ArrayList<HashSet<String>> sourceResults = new ArrayList<>();
                ArrayList<String> targetQueries = new ArrayList<>();
                ArrayList<String> sourceQueries;

                File alignmentfile = fpath.toFile();

                try {
                    if (ap.parse(alignmentfile.toURI()) instanceof EDOALAlignment) {
                        al = (BasicAlignment) ap.parse(alignmentfile.toURI());

                        File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto + "-" + targetOnto + ".edoal", targetOnto + "-" + sourceOnto + ".edoal"));
                        ensureInverseAlignmentFile(al, inverseAlignmentFile);

                        System.out.println("Translating alignment into SPARQL queries");
                        targetQueries = ((EDOALAlignment) al).toTargetSPARQLQuery();
                        sourceQueries = ((EDOALAlignment) al).toSourceSPARQLQuery();


                        System.out.println("Retrieving Source Alignment SPARQL queries results");

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

                ArrayList<Double> precisions = new ArrayList<>();
                ArrayList<Double> recalls = new ArrayList<>();
                ArrayList<Double> fMeasures = new ArrayList<>();

                AtomicReference<ArrayList<String>> rewrittenQueries = new AtomicReference<>(new ArrayList<>());
                BasicAlignment finalAl = al;
                ArrayList<String> finalTargetQueries = targetQueries;
                try(Stream<Path> files = Files.walk(Paths.get(queriesPath), 1)){
                    files.forEach(path -> {
                        evaluate(sourceOnto, targetOnto, precisions, recalls, fMeasures, sourceResults, rewrittenQueries, resultCSV, finalAl, finalTargetQueries, path);
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                resultCSV.append("global mean,").append(mean(precisions)).append(",").append(mean(fMeasures)).append(",").append(mean(recalls));


                String proportionQueries = calcResults(precisions, recalls, fMeasures);


                cvWriter.println(fpath.getFileName().toString().split("\\.")[0] + "," + resultCSV);
                writer.println(fpath.getFileName().toString().split("\\.")[0] + "," +proportionQueries);

                String[] split = proportionQueries.split(",");

                for (int i = 1; i < split.length; i++) {
                    means[i - 1] += Float.parseFloat(split[i]);
                }
                count.getAndIncrement();
            });
        }

        writer.printf("MEAN,CQAs,%f,%f,%f,%f,%f\n", means[0] / count.get(), means[1] / count.get(), means[2] / count.get(), means[3] / count.get(), means[4] / count.get());
        cvWriter.close();
        writer.close();

        DatasetManager.getInstance().close();

    }

    private static void ensureInverseAlignmentFile(Alignment al, File inverseAlignmentFile) throws AlignmentException, FileNotFoundException {
        if (!inverseAlignmentFile.exists()) {
            Alignment inverseAl = al.inverse();
            PrintWriter writer = new PrintWriter(inverseAlignmentFile);
            AlignmentVisitor renderer = new RDFRendererVisitor(writer);
            inverseAl.render(renderer);
            writer.flush();
            writer.close();
        }
    }

    private static void evaluate(String sourceOnto, String targetOnto, ArrayList<Double> precisions, ArrayList<Double> recalls, ArrayList<Double> fMeasures, ArrayList<HashSet<String>> sourceResults, AtomicReference<ArrayList<String>> rewrittenQueries, StringBuilder resultCSV, BasicAlignment finalAl, ArrayList<String> finalTargetQueries, Path path) {
        String cqa = path.getFileName().toString();
        Path sourceCQAFile = Paths.get(path + "/" + sourceOnto + ".sparql");
        Path targetCQAFile = Paths.get(path + "/" + targetOnto + ".sparql");
        if (Files.notExists(sourceCQAFile) || Files.notExists(targetCQAFile)) {
            return;
        }
        String sourceCQA = getQueryContent(sourceCQAFile);
        String targetCQA = getQueryContent(targetCQAFile);
        HashSet<String> sourceCQAresults = getSPARQLQueryResults(sourceOnto, sourceCQA);
        HashSet<String> targetCQAresults = getSPARQLQueryResults(targetOnto, targetCQA);

        if (finalAl instanceof EDOALAlignment) {
            rewrittenQueries.set(((EDOALAlignment) finalAl).rewriteAllPossibilitiesQuery(sourceCQA));
            for (int j = 0; j < sourceResults.size(); j++) {
                if (!identical(compareHashSet(sourceResults.get(j), sourceCQAresults))) { continue; }
                rewrittenQueries.get().add(finalTargetQueries.get(j));
            }
        } else {
            try {
                rewrittenQueries.get().add(finalAl.rewriteSPARQLQuery(sourceCQA));
            } catch (AlignmentException e) {
                throw new RuntimeException(e);
            }
        }

        ArrayList<Double> bestRes = new ArrayList<>();
        bestRes.add(0.0);
        bestRes.add(0.0);
        double bestFmeasure = 0.0;

        for (String rewrittenQuery : rewrittenQueries.get()) {
            HashSet<String> rewrittenResults = getSPARQLQueryResults(targetOnto, rewrittenQuery);
            ArrayList<Double> rewrittenRes = compareHashSet(targetCQAresults, rewrittenResults);
            if (fMeasure(rewrittenRes) > bestFmeasure) {
                bestRes = rewrittenRes;
                bestFmeasure = fMeasure(rewrittenRes);
            }
        }


        precisions.add(bestRes.get(0));
        recalls.add(bestRes.get(1));
        fMeasures.add(bestFmeasure);
        resultCSV.append(cqa).append(",").append(bestRes.get(0)).append(",").append(bestFmeasure).append(",").append(bestRes.get(1)).append("\n");
    }

    private static String calcResults(ArrayList<Double> precisions, ArrayList<Double> recalls, ArrayList<Double> fMeasures) {
        double nbEquivCQA = 0;
        double nbMoreGeneralCQA = 0;
        double nbMoreSpecificCQA = 0;
        double nbOverlapCQA = 0;
        for (int i = 0; i < precisions.size(); i++) {
            ArrayList<Double> result = new ArrayList<>();
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
        return (results.get(0)==1 && results.get(1) > 0) ;
    }

    public static boolean sourceMoreSpecificThanTarget(List<Double> results) {
        return 	(results.get(0)> 0 && results.get(1) ==1 ) ;
    }


    public static ArrayList<Double> compareHashSet(HashSet<String> hsource, HashSet<String> htarget) {
        ArrayList<Double> results = new ArrayList<>();
        int correctResults = 0;
        for (String targRes : htarget) {
            if (hsource.contains(targRes)) {
                correctResults++;
            }
        }
        double prec = 0;
        double rec = 0;
        if (hsource.size() > 0 && htarget.size() > 0) {
            prec = (double) correctResults / (double) htarget.size();
            rec = (double) correctResults / (double) hsource.size();
        }
        results.add(prec);
        results.add(rec);
        return results;
    }

    public static double mean(ArrayList<Double> list) {
        double res = 0;
        for (double d : list) {
            res += d;
        }
        return res / (double) list.size();
    }

    public static boolean identical(ArrayList<Double> results) {
        return results.get(0) == 1 && results.get(1) == 1;
    }

    public static boolean overlap(ArrayList<Double> results) {
        return results.get(0) > 0 && results.get(1) > 0;
    }

    public static double fMeasure(ArrayList<Double> results) {
        double prec = results.get(0);
        double rec = results.get(1);
        if (prec == 0 && rec == 0) {
            return 0;
        } else {
            return 2 * (prec * rec) / (prec + rec);
        }
    }

    public static HashSet<String> getSPARQLQueryResults(String onto, String query) {
        HashSet<String> results = new HashSet<>();
        try {

            int offset = 0;
            int limit = 10000;
            boolean end = false;
            while (!end) {
                String newQuery = query;
                newQuery += "\n LIMIT " + limit;
                newQuery += "\n OFFSET " + offset;
                List<Map<String, RDFNode>> result = sparql.SparqlProxy.query(onto, newQuery);
                Iterator<Map<String, RDFNode>> retIterator = result.iterator();
                int nbAns = 0;
                while (retIterator.hasNext()) {
                    nbAns++;
                    Map<String, RDFNode> ans = retIterator.next();
                    if (ans.containsKey("s")) {
                        String s = instanceString(ans.get("s").toString());
                        String o = "";
                        if (ans.containsKey("o")) {
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


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return results;
    }

    public static String getQueryContent(Path f) {
        StringBuilder result = new StringBuilder();
        try {
            Scanner sc = new Scanner(Files.newBufferedReader(f));

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                result.append(line).append(" ");
            }
            sc.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }


    public static String instanceString(String raw) {
        return raw.replaceAll("\"", "").replaceAll("http://[^#]+#", "").replaceAll("_v0", "").replaceAll("_v2", "");
    }

}
