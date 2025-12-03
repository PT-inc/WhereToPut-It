package cs455.amazon;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF4;

public class BaselineCalculator {
    private final SparkSession sc;
    private final String demandRegionsPath;
    private final String amazonCentersPath;
    private final String outputPath;

    public BaselineCalculator(SparkSession sc, String amazonCentersPath, String demandRegionsPath, String outputPath){
        this.sc = sc;
        this.amazonCentersPath = amazonCentersPath;
        this.demandRegionsPath = demandRegionsPath;
        this.outputPath = outputPath;
    }

    public Dataset<Row> run(){
        Dataset<Row> amazonCentersDf = sc.read().option("header", "true").csv(this.amazonCentersPath)
                                        .withColumnRenamed("Code", "centerCode")
                                        .withColumnRenamed("Type", "centerType")
                                        .withColumnRenamed("Longitude", "centerLng")
                                        .withColumnRenamed("Latitude", "centerLat");

        Dataset<Row> demandRegionsDf = sc.read().option("header", "true").csv(this.demandRegionsPath)
                                        .withColumnRenamed("lat", "demandRegionLat")
                                        .withColumnRenamed("lng", "demandRegionLng");

        Dataset<Row> cartesianProductDf = demandRegionsDf.crossJoin(amazonCentersDf);

        // Create and register the UDF
        UDF4<Double, Double, Double, Double, Double> distanceCalculator = new  DistanceCalculatorUDF();
        this.sc.udf().register("calcDistance", distanceCalculator, DataTypes.DoubleType);

        Dataset<Row> distancesToAllCentersDf = cartesianProductDf.withColumn(
            "distanceBetween",
            callUDF("calcDistance", 
                col("demandRegionLat").cast(DataTypes.DoubleType),
                col("demandRegionLng").cast(DataTypes.DoubleType),
                col("centerLat").cast(DataTypes.DoubleType),
                col("centerLng").cast(DataTypes.DoubleType)
            )
        );

        Dataset<Row> shortestDistancesDf = distancesToAllCentersDf.groupBy(col("zip"))
                                        .agg(min(col("distanceBetween")).as("distanceBetween"));

        String[] joinCols = {"zip", "distanceBetween"};
        Dataset<Row> closestCentersDf = distancesToAllCentersDf.join(shortestDistancesDf, joinCols)
                                            .withColumnRenamed("distanceBetween", "distanceFromNearestCenter")
                                            .withColumnRenamed("centerCode", "nearestCenterCode")
                                            .drop("centerType", "centerLng", "centerLat"); // Don't need these cols


        closestCentersDf = closestCentersDf.withColumnRenamed("demand_score", "demandScore");
        return closestCentersDf;
    }

}
