package cs455.amazon;
 
import org.apache.spark.sql.SparkSession;
 
public class Controller {
    public static void main(String[] args) {
        if(args.length != 5){
            System.err.println("Usage: Controller <amazonCentersPath> <demandRegionsPath> <candidateLocationsPath> <baselineOutputPath> <optimizerOutputPath>");
            System.exit(1);
        }
 
        String amazonCentersPath = args[0];
        String demandRegionsPath = args[1];
        String candidateLocationsPath = args[2];
        String baselineOutputPath = args[3];
        String optimizerOutputPath = args[4];
 
        SparkSession sc = SparkSession.builder()
                                .appName("WhereToPut-It")
                                .getOrCreate();
 
        Baseline baseline = new Baseline(sc, amazonCentersPath, demandRegionsPath, baselineOutputPath);
        baseline.run();
 
        Optimizer optimizer = new Optimizer(sc, candidateLocationsPath, baselineOutputPath, optimizerOutputPath);
        optimizer.run();
 
 
        sc.stop();
    }
 
}