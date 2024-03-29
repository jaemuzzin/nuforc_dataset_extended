package jae.muzzin.nuforc_extend;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvReader;
import de.siegmar.fastcsv.writer.CsvWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import static java.lang.System.out;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.christopherfrantz.dbscan.DBSCANClusterer;
import org.christopherfrantz.dbscan.DBSCANClusteringException;
import org.christopherfrantz.dbscan.DistanceMetric;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.nd4j.linalg.api.buffer.DataType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

/**
 *
 * @author Admin
 */
public class Nuforc_extend {

    public static final int WORD_EMBEDDING_DIMS = 128;

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        if (!new File("nuforce_latlong.csv").exists()) {
            extendCSV();
        }
        if (!new File("shapes.csv").exists()) {
            writeShapes();
        }
        if (!new File("word2vec.txt").exists()) {

            var reader = NamedCsvReader.builder().build(new FileReader("nuforc_latlong.csv"));
            IntSupplier prog = new IntSupplier() {
                int i = 0;

                @Override
                public int getAsInt() {
                    return i++;
                }
            };
            TokenizerFactory t = new DefaultTokenizerFactory();
            t.setTokenPreProcessor(new CommonPreprocessor());
            Word2Vec word2vec = new Word2Vec.Builder()
                    .layerSize(WORD_EMBEDDING_DIMS)
                    .windowSize(5)
                    .stopWords(Arrays.asList(new String[]{"the", "a", "from", "and", "then", "to", "has", "is", "had", "but", "have", "with", "it", "this", "that"}))
                    .minWordFrequency(3)
                    .tokenizerFactory(t)
                    .iterate(new SentenceIterator() {
                        private SentencePreProcessor spp = new DefaultTokenizer();

                        Iterator<String> iter = reader
                                .stream()
                                .flatMap(row -> Arrays.stream((row.getField("summary") + ". " + row.getField("text")).split("\\.")))
                                .iterator();

                        @Override
                        public String nextSentence() {
                            var s = iter.next();
                            String pps = spp.preProcess(s);
                            //out.println("reading string " + s.substring(0, Math.min(80, s.length())));
                            //out.println("final string " + pps.substring(0, Math.min(80, pps.length())));
                            int i = prog.getAsInt();
                            if (i % 10000 == 0) {
                                System.err.println(pps);
                                System.err.println(i);
                                System.err.println("===============");
                                System.err.println(" ");
                            }
                            return pps + " ";
                        }

                        @Override
                        public boolean hasNext() {
                            return iter.hasNext();
                        }

                        @Override
                        public void reset() {
                            iter = reader.stream()
                                    .flatMap(row -> Arrays.stream((row.getField("summary") + ". " + row.getField("text")).split("\\.")))
                                    .iterator();
                        }

                        @Override
                        public void finish() {

                        }

                        @Override
                        public SentencePreProcessor getPreProcessor() {
                            return spp;
                        }

                        @Override
                        public void setPreProcessor(SentencePreProcessor spp) {
                            this.spp = spp;
                        }
                    }).epochs(1)
                    .build();
            out.println("fitting word2vec.. this will take awhile..");
            word2vec.fit();
            out.println("done fitting word2vec.. writing..");
            WordVectorSerializer.writeWord2VecModel(word2vec, "word2vec.txt");
        }
        if (!new File("nuforc_numeric.csv").exists() || !new File("nuforc_numeric_sans_nlp.csv").exists()) {
            ArrayList<float[]> dataList = new ArrayList<>();
            Word2Vec word2vec = WordVectorSerializer.readWord2VecModel(new File("word2vec.txt"), true);
            var reader = NamedCsvReader.builder().build(new FileReader("nuforc_latlong.csv"));

            DefaultTokenizer spp = new DefaultTokenizer();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Random random = new Random(1234);
            String[] shapes = new String[]{"cigar", "other", "light", "circle", "triangle", "", "fireball", "oval", "disk", "unknown", "chevron", "cylinder", "diamond", "sphere", "changing", "rectangle", "formation", "egg", "star", "delta", "teardrop", "cross", "flash", "cone"};
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("nuforc_numeric.csv"));
            CsvWriter writerSansNLP = CsvWriter.builder().build(new FileWriter("nuforc_numeric_sans_nlp.csv"));
            writerSansNLP.writeRow("lat", "long", "datetime", "cigar", "other", "light", "circle", "triangle", "", "fireball", "oval", "disk", "unknown", "chevron", "cylinder", "diamond", "sphere", "changing", "rectangle", "formation", "egg", "star", "delta", "teardrop", "cross", "flash", "cone", "id");
            IntSupplier j = new IntSupplier() {
                int j = 0;

                @Override
                public int getAsInt() {
                    return j++;
                }
            };
            long roswell47 = sdf.parse("1947-01-01 00:00").getTime();
            long now = sdf.parse("2024-01-01 00:00").getTime();
            float timeScale = now - roswell47;
            float latScale = 55 - 25;
            float longScale = 123 - 63;
            reader.stream()
                    .map(row -> {
                        //words, lat, long, time, shape, id
                        double[] r = new double[WORD_EMBEDDING_DIMS + 2 + 1 + shapes.length + 1];
                        Arrays.fill(r, 0);
                        var wordList = spp.tokenize(row.getField("summary"))
                                .stream().filter(w -> word2vec.hasWord(w)).collect(Collectors.toList());
                        if (!wordList.isEmpty()) {
                            System.arraycopy(
                                    word2vec.getWordVectorsMean(wordList).toDoubleVector(),
                                    0,
                                    r,
                                    0,
                                    WORD_EMBEDDING_DIMS);
                        }
                        for (int i = 0; i < WORD_EMBEDDING_DIMS; i++) {
                            r[i] *= 0.19f; //scale it down so attributes and nlp data have comparable effect on distance calculations
                        }
                        r[WORD_EMBEDDING_DIMS] = (Float.parseFloat(row.getField("latitude")) - 25) / latScale;
                        r[WORD_EMBEDDING_DIMS + 1] = (Math.abs(Float.parseFloat(row.getField("longitude"))) - 63) / longScale;
                        try {
                            r[WORD_EMBEDDING_DIMS + 2] = (sdf.parse(row.getField("date_time")).getTime() - roswell47) / timeScale;
                        } catch (ParseException ex) {
                            r[WORD_EMBEDDING_DIMS + 2] = 0 + (float) random.nextFloat();
                        }
                        for (int i = 0; i < shapes.length; i++) {
                            r[WORD_EMBEDDING_DIMS + 3 + i] = row.getField("shape").toLowerCase().equals(shapes[i]) ? 1 : 0;
                        }
                        r[r.length - 1] = Float.parseFloat(row.getField("index"));
                        int jae = j.getAsInt();
                        if (jae % 100 == 0) {
                            System.err.println("Encoded " + jae);
                        }
                        return r;
                    })
                    .filter(r -> r != null)
                    .peek(row -> writerSansNLP.writeRow(Arrays.stream(row).skip(WORD_EMBEDDING_DIMS).mapToObj(f -> "" + f).toList().toArray(new String[0])))
                    .forEach(row -> writer.writeRow(Arrays.stream(row).mapToObj(f -> "" + f).toList().toArray(new String[0])));
            writer.close();
            writerSansNLP.close();
        }
        /*
        if (!new File("3rd-nn-dist.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("3rd-nn-dist.csv"));
            var readerOuter = CsvReader.builder().build(new FileReader("nuforc_numeric.csv"));
            readerOuter.
                    stream()
                    .limit(10000)
                    .peek(r -> System.err.print("."))
                    //.parallel()
                    .map(row -> {
                        var outerArr = Nd4j.create(row
                                .getFields()
                                .stream()
                                .limit(row.getFieldCount() - 1)
                                .mapToDouble(s -> Double.parseDouble(s))
                                .toArray());
                        try {
                            var readerInner = CsvReader.builder().build(new FileReader("nuforc_numeric.csv"));
                            double thirdNeighDist = readerInner
                                    .stream()
                                    .limit(10000)
                                    .parallel()
                                    .map(rowInner
                                            -> Nd4j.create(
                                            rowInner.getFields()
                                                    .stream()
                                                    .limit(row.getFieldCount() - 1)
                                                    .mapToDouble(s -> Double.parseDouble(s))
                                                    .toArray())
                                    ).map(innerArr -> Nd4j.math.sqrt(Nd4j.math.pow(innerArr.sub(outerArr), 2).sum()).sumNumber().doubleValue())
                                    .sorted()
                                    .skip(2)
                                    .limit(1)
                                    .findFirst().orElse(0d);
                            return new double[]{Double.parseDouble(row.getField(row.getFieldCount() - 1)), thirdNeighDist};
                        } catch (FileNotFoundException ex) {
                            Logger.getLogger(Nuforc_extend.class.getName()).log(Level.SEVERE, null, ex);
                            return new double[]{0, 0};
                        }
                    }
                    )
                    //.sequential()
                    .sorted((x, y) -> new Double(y[1]).compareTo(new Double(x[1])))
                    .forEach(r -> writer.writeRow("" + ((int) r[0]), "" + r[1]));
            System.err.println(".");
            writer.close();
        }
        if (!new File("dbscan.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("dbscan.csv"));
            writer.writeRow("id", "cluster");

            var reader = CsvReader.builder().build(new FileReader("nuforc_numeric.csv"));
            var data = reader.stream().map(r -> Nd4j.create(r.getFields().stream().mapToDouble(s -> Double.parseDouble(s)).toArray()))
                    .map(arr -> new DBScanRecord((int)arr.getDouble(arr.shape()[0] - 1), arr))
                    .toList();
            var mask = Nd4j.ones(DataType.DOUBLE, data.get(0).data.shape()[0]);
            mask.putScalar(mask.shape()[0]-1, 0d);
            try {
                DBSCANClusterer<DBScanRecord> dbscan = new DBSCANClusterer<>(data, 3, .008, (DBScanRecord val1, DBScanRecord val2) -> val2.data.mul(mask).distance2(val1.data.mul(mask)));
                System.err.println("Performing dbscan");
                var dbscanResult = dbscan.performClustering();
                IntSupplier i = new IntSupplier() {
                    int i = 0;

                    @Override
                    public int getAsInt() {
                        return this.i++;
                    }
                };
                dbscanResult.
                    stream()
                    .flatMap(r -> {
                        int id = i.getAsInt();
                        return r.stream().map(arr -> new String[]{"" + (int)arr.id, ""+id});
                    })
                    .forEach(row -> writer.writeRow(row));
            } catch (DBSCANClusteringException ex) {
                Logger.getLogger(Nuforc_extend.class.getName()).log(Level.SEVERE, null, ex);
            }
            writer.close();
        }*/
        if (!new File("kmeans_summary.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("kmeans_summary.csv"));
            writer.writeRow("clusters", "wcss");
            for (int k = 10000; k < 30000; k += 1000) {
                var r = K_Clusterer.cluster(k, "nuforc_numeric_sans_nlp.csv", true, 20);
                System.err.println("" + k + ", " + "" + r.wcss);
                writer.writeRow("" + k, "" + r.wcss);
            }
            writer.close();
        }
        if (!new File("kmeans_16k_clusters.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("kmeans_16k_clusters.csv"));
            writer.writeRow("id", "cluster");
            var r = K_Clusterer.cluster(16000, "nuforc_numeric_sans_nlp.csv", true, 50);
            r.idToCluster
                    .entrySet()
                    .stream()
                    .forEach(e -> writer.writeRow("" + e.getKey(), "" + e.getValue()));
            writer.close();
        }
        if (!new File("kmeans_summary_nlp.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("kmeans_summary_nlp.csv"));
            writer.writeRow("clusters", "wcss");
            for (int k = 10000; k < 30000; k += 1000) {
                var r = K_Clusterer.cluster(k, "nuforc_numeric.csv", true, 20);
                System.err.println("" + k + ", " + "" + r.wcss);
                writer.writeRow("" + k, "" + r.wcss);
            }
            writer.close();
        }
        if (!new File("kmeans_16k_clusters_nlp.csv").exists()) {
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("kmeans_16k_clusters_nlp.csv"));
            writer.writeRow("id", "cluster");
            var r = K_Clusterer.cluster(16000, "nuforc_numeric.csv", true, 2000);
            r.idToCluster
                    .entrySet()
                    .stream()
                    .forEach(e -> writer.writeRow("" + e.getKey(), "" + e.getValue()));
            writer.close();
        }
    }

    public static void writeShapes() throws FileNotFoundException, IOException {
        var reader = NamedCsvReader.builder().build(new FileReader("nuforc_latlong.csv"));

        CsvWriter writer = CsvWriter.builder().build(new FileWriter("shapes.csv"));
        reader.stream()
                .map(r -> r.getField("shape").toLowerCase())
                .distinct()
                .forEach(shape -> writer.writeRow(shape));
        reader.close();
        writer.close();
    }

    public static void extendCSV() throws FileNotFoundException, IOException {
        final HashMap<String, Map<String, float[]>> geocoder = new HashMap<>();
        var georeader = NamedCsvReader.builder().build(new FileReader("uscities.csv"));
        georeader.stream()
                .peek(r -> geocoder.putIfAbsent(r.getField("state_id").toLowerCase(), new HashMap<>()))
                .forEach(r -> geocoder.get(r.getField("state_id").toLowerCase()).put(r.getField("city").toLowerCase(),
                new float[]{Float.parseFloat(r.getField("lat")), Float.parseFloat(r.getField("lng"))}));
        georeader = NamedCsvReader.builder().build(new FileReader("canadacities.csv"));

        georeader.stream()
                .peek(r -> geocoder.putIfAbsent(r.getField("province_id").toLowerCase(), new HashMap<>()))
                .forEach(r -> geocoder.get(r.getField("province_id").toLowerCase()).put(r.getField("city").toLowerCase(),
                new float[]{Float.parseFloat(r.getField("lat")), Float.parseFloat(r.getField("lng"))}));
        var reader = NamedCsvReader.builder().build(new FileReader("nuforc_reports.csv"));

        CsvWriter writer = CsvWriter.builder().build(new FileWriter("nuforc_latlong.csv"));
        String[] headers = new String[]{"level_0", "index", "date_time", "city", "state", "country", "posted", "latitude", "longitude", "shape", "duration", "summary", "text"};
        writer.writeRow(headers);
        IntSupplier ids = new IntSupplier() {
            private int id = 0;

            @Override
            public int getAsInt() {
                return id++;
            }
        };
        reader.stream()
                .map(row -> new HashMap<String, String>(row.getFields()))
                //.peek(rm -> System.err.print(rm.toString()))
                .filter(rowmap -> geocoder.containsKey(rowmap.get("state").toLowerCase()))
                .filter(r -> r.get("country").equals("USA") || r.get("country").equals("Canada"))
                .peek(rm -> rm.put("city", rm.get("city").replace("(Canada)", "").trim().toLowerCase()))
                .filter(rowmap -> geocoder.get(rowmap.get("state").toLowerCase()).containsKey(rowmap.get("city")))
                .peek(rowmap -> rowmap.put("latitude", "" + geocoder.get(rowmap.get("state").toLowerCase()).get(rowmap.get("city"))[0]))
                .peek(rowmap -> rowmap.put("longitude", "" + geocoder.get(rowmap.get("state").toLowerCase()).get(rowmap.get("city"))[1]))
                .peek(rowmap -> rowmap.remove("stats"))
                .peek(r -> r.remove("report_link"))
                .peek(rm -> rm.put("level_0", ids.getAsInt() + ""))
                .peek(rm -> rm.put("index", rm.get("level_0")))
                .peek(rm -> rm.put("text", rm.get("text").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .peek(rm -> rm.put("summary", rm.get("summary").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .forEach(rowmap -> writer.writeRow(Arrays.stream(headers).map(h -> rowmap.get(h)).collect(Collectors.toList()).toArray(new String[0])));
        reader.close();
        reader = NamedCsvReader.builder().build(new FileReader("nuforc_reports.csv"));
        Random random = new Random(1234);
        reader.stream()
                .map(row -> new HashMap<String, String>(row.getFields()))
                //.peek(rm -> System.err.print(rm.toString()))
                .filter(rowmap -> geocoder.containsKey(rowmap.get("state").toLowerCase()))
                .filter(r -> r.get("country").equals("USA") || r.get("country").equals("Canada"))
                .peek(rm -> rm.put("city", rm.get("city").replace("(Canada)", "").trim().toLowerCase()))
                .filter(rowmap -> !geocoder.get(rowmap.get("state").toLowerCase()).containsKey(rowmap.get("city")))
                .peek(rowmap -> rowmap.put("latitude", "" + (new ArrayList<Map.Entry<String, float[]>>(geocoder.get(rowmap.get("state").toLowerCase()).entrySet()).get(random.nextInt(geocoder.get(rowmap.get("state").toLowerCase()).size())).getValue()[0] + random.nextGaussian(0, .5))))
                .peek(rowmap -> rowmap.put("longitude", "" + (new ArrayList<Map.Entry<String, float[]>>(geocoder.get(rowmap.get("state").toLowerCase()).entrySet()).get(random.nextInt(geocoder.get(rowmap.get("state").toLowerCase()).size())).getValue()[1] + random.nextGaussian(0, .5))))
                .peek(rowmap -> rowmap.remove("stats"))
                .peek(r -> r.remove("report_link"))
                .peek(rm -> rm.put("level_0", ids.getAsInt() + ""))
                .peek(rm -> rm.put("index", rm.get("level_0")))
                .peek(rm -> rm.put("text", rm.get("text").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .peek(rm -> rm.put("summary", rm.get("summary").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .forEach(rowmap -> writer.writeRow(Arrays.stream(headers).map(h -> rowmap.get(h)).collect(Collectors.toList()).toArray(new String[0])));
        writer.close();
    }
    public static class DBScanRecord{
        int id;
        INDArray data;

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + this.id;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DBScanRecord other = (DBScanRecord) obj;
            return this.id == other.id;
        }

        public DBScanRecord(int id, INDArray data) {
            this.id = id;
            this.data = data;
        }
    }
}
