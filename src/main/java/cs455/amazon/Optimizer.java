package cs455.amazon;

public class Optimizer {
    private final SparkSession sc;
    private final String candidateLocationsPath;
    private final String baselinePath;
    private final String outputPath;

    public Optimizer(SparksSession sc, String candidateLocationsPath, String baselinePath, String outputPath){
        this.sc = sc;
        this.candidateLocationsPath = candidateLocationsPath;
        this.baselinePath = baselinePath;
        this.outputPath = outputPath;
    }
}