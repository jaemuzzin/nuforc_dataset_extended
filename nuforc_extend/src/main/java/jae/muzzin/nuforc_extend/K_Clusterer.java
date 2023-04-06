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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class K_Clusterer extends ReadDataset {

    public static class ReturnValue {

        public ReturnValue(Map<Long, Integer> idToCluster, double wcss) {
            this.idToCluster = idToCluster;
            this.wcss = wcss;
        }

        public Map<Long, Integer> idToCluster;
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
        System.err.println("Num features " + r1.numberOfFeatures);

        int k = numClusters;
        int distance = manhattan ? 2 : 1;
        //Hashmap to store centroids with index
        Map<Integer, double[]> centroids = new ConcurrentHashMap<>();
        // calculating initial centroids
        double[] x1;
        int r = 0;
        for (int i = 0; i < k; i++) {
            x1 = r1.getFeatures().get(r++);
            centroids.put(i, x1);
        }
        //reassigning to new clusters
        Map<Row, Integer> clusters = new HashMap<>();
        for (int i = 0; i < max_iterations; i++) {
            System.err.print(".");
            var clustersIter = kmeans(r1.getFeatures(), r1.getLabel(), distance, centroids, k);
            IntStream.range(0,k)
                    .parallel()
                    .forEach(j -> {
                        List<double[]> list = new ArrayList<>();
                        for (Row key : clustersIter.keySet()) {
                            if (clustersIter.get(key) == j) {
                                list.add(key.data);
                            }
                        }
                        centroids.put(j, centroidCalculator(list, r1.numberOfFeatures));
                    });
            clusters = clustersIter;
        }
        System.err.println(".");
        HashMap<Long, Integer> idToCluster = new HashMap<>();
        for (Row key : clusters.keySet()) {
            idToCluster.put((long)Float.parseFloat(key.id), clusters.get(key));
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
        return new ReturnValue(idToCluster, wcss/numClusters);
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
        Map<Row, Integer> clusters = new ConcurrentHashMap<>();

        class IdToDist {

            public IdToDist(int id, double dist) {
                this.id = id;
                this.dist = dist;
            }

            int id;
            double dist;
        };
        IntStream.range(0, features.size())
                .parallel()
                .forEach(i -> {
                    var x = features.get(i);
                    int k1 = IntStream.range(0, k)
                            .mapToObj(j -> new IdToDist(j, Distance.eucledianDistance(centroids.get(j), x)))
                            .sorted((z, y) -> new Double(z.dist).compareTo(new Double(y.dist)))
                            .findFirst().orElseThrow().id;
                    clusters.put(new Row(x, labels.get(i)), k1);
                });

        return clusters;
    }
}
