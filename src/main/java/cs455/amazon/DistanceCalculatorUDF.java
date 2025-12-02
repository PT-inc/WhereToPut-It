package cs455.amazon;

import org.apache.spark.sql.api.java.UDF4;

public class DistanceCalculatorUDF implements UDF4<Double, Double, Double, Double, Double>{
    @Override
    public Double call(Double lat1, Double lng1, Double lat2, Double lng2) throws Exception{
        final Double earthRadius = 3958.8;

        Double lat1Radians = Math.toRadians(lat1);
        Double lng1Radians = Math.toRadians(lng1);

        Double lat2Radians = Math.toRadians(lat2);
        Double lng2Radians = Math.toRadians(lng2);

        Double dlng = lng2Radians - lng1Radians;
        Double dlat = lat2Radians - lat1Radians;

        Double a = Math.pow(Math.sin(dlat / 2), 2) 
            + Math.cos(lat1Radians) * Math.cos(lat2Radians) * Math.pow(Math.sin(dlng / 2), 2);

        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

        Double dist = earthRadius * c;

        return dist;
    }
}