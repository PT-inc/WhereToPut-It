package cs455.amazon;
 
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import cs455.amazon.BaselineCalculator;
import cs455.amazon.Optimizer;
 
public class Controller {
    public static void main(String[] args) {
        if(args.length != 4){
            System.err.println("Usage: Controller <amazonCentersPath> <demandRegionsPath> <optimizerOutputPath> <numCandidates>");
            System.exit(1);
        }
 
        String amazonCentersPath = args[0];
        String demandRegionsPath = args[1];
        String optimizerOutputPath = args[2];
        int numCandidates = Integer.valueOf(args[3]);
 
        SparkSession sc = SparkSession.builder()
                                .appName("WhereToPut-It")
                                .getOrCreate();
 
        BaselineCalculator baseline = new BaselineCalculator(sc, amazonCentersPath, demandRegionsPath);
        Dataset<Row> baselineDf = baseline.run();

        CandidateGenerator generator = new CandidateGenerator(sc, demandRegionsPath);
        Dataset<Row> candidateDf = generator.generateCandidateDf(numCandidates);
 
        Optimizer optimizer = new Optimizer(sc, candidateDf, baselineDf, optimizerOutputPath);
        optimizer.run();
 
        sc.stop();
    }
 
}