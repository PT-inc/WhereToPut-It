package cs455.amazon;
 
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import cs455.amazon.BaselineCalculator;
import cs455.amazon.Optimizer;
 
public class Controller {
    public static void main(String[] args) {
        if(args.length != 5){
            System.err.println("Usage: Controller <amazonCentersPath> <demandRegionsPath> <baselineOutputPath> <optimizerOutputPath> <numCandidates>");
            System.exit(1);
        }
 
        String amazonCentersPath = args[0];
        String demandRegionsPath = args[1];
        String baselineOutputPath = args[2];
        String optimizerOutputPath = args[3];
        int numCandidates = Integer.valueOf(args[4]);
 
        SparkSession sc = SparkSession.builder()
                                .appName("WhereToPut-It")
                                .getOrCreate();
 
        BaselineCalculator baseline = new BaselineCalculator(sc, amazonCentersPath, demandRegionsPath, baselineOutputPath);
        Dataset<Row> baselineDf = baseline.run();

        CandidateGenerator generator = new CandidateGenerator(sc, demandRegionsPath);
        Dataset<Row> candidateDf = generator.generateCandidateDf(numCandidates);
 
        Optimizer optimizer = new Optimizer(sc, candidateDf, baselineDf, optimizerOutputPath);
        optimizer.run();
 
        sc.stop();
    }
 
}