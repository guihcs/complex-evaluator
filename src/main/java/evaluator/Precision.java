package evaluator;

import dataset.DatasetManager;
import fr.inrialpes.exmo.align.impl.BasicAlignment;
import fr.inrialpes.exmo.align.impl.edoal.EDOALAlignment;
import fr.inrialpes.exmo.align.impl.renderer.RDFRendererVisitor;
import fr.inrialpes.exmo.align.parser.AlignmentParser;
import org.semanticweb.owl.align.AlignmentVisitor;
import org.semanticweb.owl.align.Cell;
import sparql.SparqlProxy;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Precision {

	public static void main(String[] args) {
		try {
			File alignmentfile = new File("edas-conference.edoal");

//			String resultfile = "results/";
//			if(args.length>1) {
//				resultfile=args[1];
//			}
			AlignmentParser ap=new AlignmentParser();
			BasicAlignment al= new BasicAlignment();
			ArrayList<String> targetQueries= new ArrayList<String>();
			ArrayList<String> sourceQueries= new ArrayList<String>();
			String sourceOnto;
			String targetOnto;

			if(ap.parse(alignmentfile.toURI()) instanceof EDOALAlignment) {
				al=(EDOALAlignment) ap.parse(alignmentfile.toURI());
				sourceOnto = alignmentfile.getName().replaceAll("-[A-Za-z]+.edoal", "");
				targetOnto = alignmentfile.getName().replaceAll("[A-Za-z]+-", "").replaceAll(".edoal", "");
				//If inverse alignment file does not exist, invert the alignment and create it
				File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto+"-"+targetOnto+".edoal", targetOnto+"-"+sourceOnto+".edoal"));
				if(!inverseAlignmentFile.exists()) {
					EDOALAlignment inverseAl = (EDOALAlignment) al.inverse();
					PrintWriter writer = new PrintWriter (inverseAlignmentFile); 
					AlignmentVisitor renderer = new RDFRendererVisitor(writer); 
					inverseAl.render(renderer); 
					writer.flush(); 
					writer.close();
				}
				targetQueries = ((EDOALAlignment) al).toTargetSPARQLQuery();
				sourceQueries = ((EDOALAlignment) al).toSourceSPARQLQuery();
			}
			else {
				al= (BasicAlignment) ap.parse(alignmentfile.toURI());
				sourceOnto = alignmentfile.getName().replaceAll("-[A-Za-z]+.rdf", "");
				targetOnto = alignmentfile.getName().replaceAll("[A-Za-z]+-", "").replaceAll(".rdf", "");
				File inverseAlignmentFile = new File(alignmentfile.getPath().replaceAll(sourceOnto+"-"+targetOnto+".rdf", targetOnto+"-"+sourceOnto+".rdf"));
				if(!inverseAlignmentFile.exists()) {
					BasicAlignment inverseAl = (BasicAlignment) al.inverse();
					PrintWriter writer = new PrintWriter (inverseAlignmentFile); 
					AlignmentVisitor renderer = new RDFRendererVisitor(writer); 
					inverseAl.render(renderer); 
					writer.flush(); 
					writer.close();
				}
				for( Cell cell : al ){
					URI uri1 = cell.getObject1AsURI(al);
					URI uri2 = cell.getObject2AsURI(al);

					if (SparqlProxy.sendAskQuery(sourceOnto, "ASK{ ?x a <"+uri1+">}") || SparqlProxy.sendAskQuery(targetOnto, "ASK{ ?x a <"+uri2+">}")) {
						sourceQueries.add("SELECT DISTINCT ?s WHERE { ?s a <"+uri1+">.}" );
						targetQueries.add("SELECT DISTINCT ?s WHERE { ?s a <"+uri2+">.}" );
					}
					else {
						sourceQueries.add("SELECT DISTINCT ?s ?o WHERE { ?s <"+uri1+"> ?o.}" );
						targetQueries.add("SELECT DISTINCT ?s ?o WHERE { ?s <"+uri2+"> ?o.}" );
					}
				}
			}

			DatasetManager.getInstance().load(sourceOnto, "source_temp.ttl");
			DatasetManager.getInstance().load(targetOnto, "target_temp.ttl");


			double numEquiv = 0;
			double nbMoreGeneral = 0; //subs no equiv
			double nbMoreSpecific = 0; //subs no equiv
			double numOverlap = 0; //overlap no subs
			double numDisjoint = 0;
			double numEmpty = 0;
			ArrayList<Double> fmeasures= new ArrayList<Double>();

			//for each source and target query, get all answers
			ArrayList<HashSet<String>> sourceResults = new ArrayList<HashSet<String>>();
			ArrayList<HashSet<String>> targetResults = new ArrayList<HashSet<String>>();
			for(int i = 0; i< targetQueries.size();i++) {
//				System.out.println(sourceQueries.get(i));
//				System.out.println(targetQueries.get(i));
				sourceResults.add(Evaluator.getSPARQLQueryResults(sourceOnto,sourceQueries.get(i)));
				targetResults.add(Evaluator.getSPARQLQueryResults(targetOnto,targetQueries.get(i)));
				ArrayList<Double> comp = Evaluator.compareHashSet(sourceResults.get(i), targetResults.get(i));
//				System.out.println(comp.get(0)+"-"+comp.get(1));
//				System.out.println(sourceResults.get(i).size()+"-"+targetResults.get(i).size());

				fmeasures.add(Evaluator.fMeasure(comp));
				if(sourceResults.get(i).isEmpty() && targetResults.get(i).isEmpty()) {
					numEmpty++;
				}
				else {
					if (Evaluator.identical(comp)) {
						numEquiv++;
					}
					else if (sourceMoreSpecificThanTarget(comp)) {
						nbMoreGeneral++;
					}
					else if (sourceMoreGeneralThanTarget(comp)) {
						nbMoreSpecific++;
					}
					else if(Evaluator.overlap(comp)) {
						numOverlap ++;
					}
					else if(disjoint(comp)) {
						numDisjoint ++;
						//System.out.println(sourceQueries.get(i) + " " + targetQueries.get(i));
					}
				}
			}
			String resultCSV=numEquiv/sourceResults.size()+","+
					(numEquiv+nbMoreGeneral+0.5*nbMoreSpecific)/sourceResults.size()+","+
							(numEquiv+0.5*nbMoreGeneral+nbMoreSpecific)/sourceResults.size()+","+
							(numEquiv+nbMoreGeneral+nbMoreSpecific+numOverlap)/sourceResults.size()+","+
							Evaluator.mean(fmeasures)+","+(numEquiv+nbMoreGeneral+nbMoreSpecific+numOverlap+numEmpty)/sourceResults.size();
							
			//resultCSV = targetQueries.size()+","+numEquiv+","+(nbMoreSpecific+nbMoreGeneral)+","+numOverlap+","+numDisjoint+","+numEmpty;
			System.out.println(resultCSV);
			/*try {
				PrintWriter writer = new PrintWriter(resultfile+sourceOnto+"-"+targetOnto+".csv", "UTF-8");
				writer.println(resultCSV);
				writer.close(); 
			} catch (FileNotFoundException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}*/


		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}



	public static boolean disjoint(ArrayList<Double> results) {
		return results.get(0) == 0 && results.get(1) == 0;
	}

	public static boolean sourceMoreGeneralThanTarget(List<Double> results) {
		return (results.get(0)==1 && results.get(1) > 0) ;
	}

	public static boolean sourceMoreSpecificThanTarget(List<Double> results) {
		return 	(results.get(0)> 0 && results.get(1) ==1 ) ;
	}

}
