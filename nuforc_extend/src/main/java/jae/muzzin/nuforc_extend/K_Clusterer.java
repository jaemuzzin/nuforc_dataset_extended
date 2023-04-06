package jae.muzzin.nuforc_extend;


/*
 * Programmed by Shephalika Shekhar
 * Class for Kmeans Clustering implemetation
 */
import jae.muzzin.nuforc_extend.kmeans.Distance;
import jae.muzzin.nuforc_extend.kmeans.ReadDataset;
import jae.muzzin.nuforc_extend.kmeans.Row;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class K_Clusterer extends ReadDataset {

    public static class ReturnValue{

        public ReturnValue(Map<Integer, Integer> idToCluster, double wcss) {
            this.idToCluster = idToCluster;
            this.wcss = wcss;
        }
        
        public Map<Integer, Integer> idToCluster;
        public double wcss;
    }
    public K_Clusterer() {
        // TODO Auto-generated constructor stub
    }

    public static ReturnValue cluster(int numClusters, String csvPath, boolean manhattan, int max_iterations) throws IOException {
        System.err.println("Clustering " + numClusters);
        ReadDataset r1 = new ReadDataset();
        r1.getFeatures().clear();
        r1.read(csvPath); //load data
        System.err.println("Clustering " + r1.numberOfFeatures);
        
        int k = numClusters;
        int distance = manhattan ? 2 : 1;
        //Hashmap to store centroids with index
        Map<Integer, double[]> centroids = new HashMap<>();
        // calculating initial centroids
        double[] x1;
        int r = 0;
        for (int i = 0; i < k; i++) {
            x1 = r1.getFeatures().get(r++);
            centroids.put(i, x1);
        }
        //Hashmap for finding cluster indexes
        Map<Row, Integer> clusters = new HashMap<>();
        clusters = kmeans(r1.getFeatures(), r1.getLabel(), distance, centroids, k);
        // initial cluster print
        for (Row key : clusters.keySet()) {
            //for (int i = 0; i < key.data.length; i++) {
            System.out.print(key.id + ", ");
            //}
            System.out.print(clusters.get(key) + "\n");
        }
        double db[];
        //reassigning to new clusters
        for (int i = 0; i < max_iterations; i++) {
            for (int j = 0; j < k; j++) {
                List<double[]> list = new ArrayList<>();
                for (Row key : clusters.keySet()) {
                    if (clusters.get(key) == j) {
                        list.add(key.data);
                    }
                }
                db = centroidCalculator(list, r1.numberOfFeatures);
                centroids.put(j, db);

            }
            clusters.clear();
            clusters = kmeans(r1.getFeatures(), r1.getLabel(), distance, centroids, k);

        }

        //final cluster print
        System.out.println("\nFinal Clustering of Data");
        System.out.println("Feature1\tFeature2\tFeature3\tFeature4\tCluster");
        HashMap<Integer, Integer> idToCluster = new HashMap<>();
        for (Row key : clusters.keySet()) {
            idToCluster.put(Integer.parseInt(key.id), clusters.get(key));
        }
        //System.out.println("Centroids:");
        for (double[] centroid : centroids.values()) {
            //System.out.println(Arrays.toString(centroid));

        }

        //Calculate WCSS
        double wcss = 0;

        for (int i = 0; i < k; i++) {
            double sse = 0;
            for (Row key : clusters.keySet()) {
                if (clusters.get(key) == i) {
                    sse += Math.pow(Distance.eucledianDistance(key.data, centroids.get(i)), 2);
                }
            }
            wcss += sse;
        }
        return new ReturnValue(idToCluster, wcss);
    }

    //method to calculate centroids
    public static double[] centroidCalculator(List<double[]> a, int numFeatures) {

        int count = 0;
        //double x[] = new double[ReadDataset.numberOfFeatures];
        double sum = 0.0;
        double[] centroids = new double[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            sum = 0.0;
            count = 0;
            for (double[] x : a) {
                count++;
                sum = sum + x[i];
            }
            centroids[i] = sum / count;
        }
        return centroids;

    }

    //method for putting features to clusters and reassignment of clusters.
    public static Map<Row, Integer> kmeans(List<double[]> features, List<String> labels, int distance, Map<Integer, double[]> centroids, int k) {
        Map<Row, Integer> clusters = new HashMap<>();
        int k1 = 0;
        double dist = 0.0;
        for (int i = 0; i < features.size(); i++) {
            double[] x = features.get(i);
            double minimum = 999999.0;
            for (int j = 0; j < k; j++) {
                if (distance == 1) {
                    dist = Distance.eucledianDistance(centroids.get(j), x);
                } else if (distance == 2) {
                    dist = Distance.manhattanDistance(centroids.get(j), x);
                }
                if (dist < minimum) {
                    minimum = dist;
                    k1 = j;
                }

            }
            clusters.put(new Row(features.get(i), labels.get(i)), k1);
        }

        return clusters;
    }
}
