package cs455.amazon;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The responsibility of this class is to generate a dataframe of candidate centers.
 * It does this by getting a random sample of us zip codes
 */
public class CandidateGenerator {
    private final SparkSession sc;
    private final String zipCodesPath;

    public CandidateGenerator(SparkSession sc, String zipCodesPath){
        this.sc = sc;
        this.zipCodesPath = zipCodesPath;
    }

    public Dataset<Row> generateCandidateDf(int numCandidates){
        Dataset<Row> allZipcodesDf = sc.read().option("header", "true").csv(this.zipCodesPath)
                                        .select(
                                            col("zip").as("candidateID"),
                                            col("lat").as("candidateLat"),
                                            col("lng").as("candidateLng")
                                        );

        Dataset<Row> randCandidateSamlple  = allZipcodesDf.orderBy(rand()).limit(numCandidates);

        return randCandidateSamlple;
    }
}