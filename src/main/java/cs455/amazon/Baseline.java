package cs455.amazon;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF4;

public class Baseline {
    private SparkSession sc;
    private String inputPath;
    private String outputPath;

    public Baseline(SparkSession sc, String amazonCentersPath, String demandRegionsPath, String outputPath){
        this.sc = sc;
        this.amazonCentersPath = amazonCentersPath;
        this.demandRegionsPath = demandRegionsPath;
        this.outputPath = outputPath;
    }

    public void run(){
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
        UDF4<Double, Double, Double, Double> distanceCalculator = new  DistanceCalculatorUDF<>();
        this.sc.udf().register("calcDistance", distanceCalculator, DataTypes.DoubleType);

        Dataset<Row> distancesToAllCentersDf = cartesianProductDf.withColumn(
            "distance_between",
            callUDF("calcDistance", 
                col("demandRegionLat").cast(DataTypes.DoubleType),
                col("demandRegionLng").cast(DataTypes.DoubleType),
                col("centerLat").cast(DataTypes.DoubleType),
                col("centerLng").cast(DataTypes.DoubleType)
            )
        );

        Dataset<Row> shortestDistancesDf = distancesToAllCentersDf.groupBy(col("zip"))
                                        .agg(min(col("distance_between")).as("distance_between"));

        String[] joinCols = {"zip", "distance_between"};
        Dataset<Row> closestCentersDf = distancesToAllCentersDf.join(shortestDistancesDf, joinCols)
                                            .withColumnRenamed("distance_between", "distance_from_nearest_center")
                                            .withColumnRenamed("centerCode", "nearestCenterCode")
                                            .drop("centerType", "centerLng", "centerLat"); // Don't need these cols


        closestCentersDf.write().option("header", "true").mode("append").csv(this.outputPath);
    }

}
