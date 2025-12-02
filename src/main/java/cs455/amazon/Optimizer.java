package cs455.amazon;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF4;

public class Optimizer {
    private final SparkSession sc;
    private final String candidateLocationsPath;
    private final String baselinePath;
    private final String outputPath;

    public Optimizer(SparkSession sc, String candidateLocationsPath, String baselinePath, String outputPath){
        this.sc = sc;
        this.candidateLocationsPath = candidateLocationsPath;
        this.baselinePath = baselinePath;
        this.outputPath = outputPath;
    }

    public void run(){
        Dataset<Row> candidateLocationsDf = sc.read().option("header", "true").csv(this.candidateLocationsPath);

        Dataset<Row> baselineDf = sc.read().option("header", "true").csv(this.baselinePath)
            .withColumn("distanceFromNearestCenter", col("distanceFromNearestCenter").cast(DataTypes.DoubleType))
            .withColumn("demandScore", col("demandScore").cast(DataTypes.DoubleType));

        Dataset<Row> cartesianProductDf = baselineDf.crossJoin(candidateLocationsDf);

        // Create and register the UDF
        UDF4<Double, Double, Double, Double> distanceCalculator = new  DistanceCalculatorUDF<>();
        this.sc.udf().register("calcDistance", distanceCalculator, DataTypes.DoubleType);

        Dataset<Row> distanceToCandidatesDf = cartesianProductDf.withColumn(
            "distanceToCandidate",
            callUDF("calcDistance", 
                col("demandRegionLat").cast(DataTypes.DoubleType),
                col("demandRegionLng").cast(DataTypes.DoubleType),
                col("candidateLat").cast(DataTypes.DoubleType),
                col("candidateLng").cast(DataTypes.DoubleType)
            )
        );

        // Improvement = max(0, distanceFromNearestCenter - distanceToCandidateColumn)
        Dataset<Row> improvementsDf = distanceToCandidatesDf.withColumn(
            "improvement",
            when(
                col("distanceFromNearestCenter").minus(col("distanceToCandidate")).gt(0),
                col("distanceFromNearestCenter").minus(col("distanceToCandidate"))
            ).otherwise(0)
        );

        // Weight the improvement by the regions demand score
        Dataset<Row> weightedImprovementsDf = improvementsDf.withColumn(
            "weightedImprovement",
            col("improvement").multiply(col("demandScore"))
        );

        Dataset<Row> totalImprovementDf = weightedImprovementsDf.groupby(col("candidateID"))
            .agg(sum(col("weightedImprovement")).as("totalImprovement"));
        
        Dataset<Row> bestCandidate = totalImprovementDf.orderBy(
            col("totalImprovement").desc()
        ).limit(1);

        Dataset<Row> bestCandidateDetails = bestCandidate.join(candidateLocationsDf, "candidateID");
        bestCandidateDetails.write().option("header", "true").mode("append").csv(this.outputPath);
    }
}