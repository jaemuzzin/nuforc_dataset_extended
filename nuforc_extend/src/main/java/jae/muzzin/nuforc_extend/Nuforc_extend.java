package jae.muzzin.nuforc_extend;

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
import java.util.Map;
import java.util.Random;
import java.util.function.IntSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

/**
 *
 * @author Admin
 */
public class Nuforc_extend {

    public static void main(String[] args) throws FileNotFoundException, IOException {
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
                    .layerSize(256)
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
        if (!new File("nuforc_numeric.csv").exists()) {
            ArrayList<float[]> dataList = new ArrayList<>();
            Word2Vec word2vec = WordVectorSerializer.readWord2VecModel(new File("word2vec.txt"), true);
            var reader = NamedCsvReader.builder().build(new FileReader("nuforc_latlong.csv"));

            DefaultTokenizer spp = new DefaultTokenizer();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Random random = new Random(1234);
            String[] shapes = new String[]{"cigar", "other", "light", "circle", "triangle", "", "fireball", "oval", "disk", "unknown", "chevron", "cylinder", "diamond", "sphere", "changing", "rectangle", "formation", "egg", "star", "delta", "teardrop", "cross", "flash", "cone"};
            CsvWriter writer = CsvWriter.builder().build(new FileWriter("nuforc_numeric.csv"));
            IntSupplier j = new IntSupplier() {
                int j = 0;

                @Override
                public int getAsInt() {
                    return j++;
                }
            };
            reader.stream()
                    .map(row -> {
                        //words, lat, long, time, shape
                        double[] r = new double[256 + 2 + 1 + shapes.length];
                        System.arraycopy(
                                word2vec.getWordVectorsMean(spp.tokenize(row.getField("summary") + ". " + row.getField("text"))
                                        .stream().filter(w -> word2vec.hasWord(w)).collect(Collectors.toList())).toDoubleVector(),
                                0,
                                r,
                                0,
                                256);
                        r[257] = Float.parseFloat(row.getField("latitude"));
                        r[258] = Float.parseFloat(row.getField("longitude"));
                        try {
                            r[259] = sdf.parse(row.getField("date_time")).getTime();
                        } catch (ParseException ex) {
                            r[259] = 0 + (float) random.nextGaussian(0, 1e9);
                        }
                        for (int i = 0; i < shapes.length; i++) {
                            r[260 + i] = row.getField("shape").toLowerCase().equals(shapes[i]) ? 1 : 0;
                        }
                        int jae = j.getAsInt();
                        if (jae % 100 == 0) {
                            System.err.println("Encoded " + jae);
                        }
                        return r;
                    }).forEach(row -> writer.writeRow(Arrays.stream(row).mapToObj(f -> "" + f).toList().toArray(new String[0])));
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
                .peek(rowmap -> rowmap.put("latitude", "" + new ArrayList<Map.Entry<String, float[]>>(geocoder.get(rowmap.get("state").toLowerCase()).entrySet()).get(0).getValue()[0] + random.nextGaussian(0, 1)))
                .peek(rowmap -> rowmap.put("longitude", "" + new ArrayList<Map.Entry<String, float[]>>(geocoder.get(rowmap.get("state").toLowerCase()).entrySet()).get(0).getValue()[1] + random.nextGaussian(0, 1)))
                .peek(rowmap -> rowmap.remove("stats"))
                .peek(r -> r.remove("report_link"))
                .peek(rm -> rm.put("level_0", ids.getAsInt() + ""))
                .peek(rm -> rm.put("index", rm.get("level_0")))
                .peek(rm -> rm.put("text", rm.get("text").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .peek(rm -> rm.put("summary", rm.get("summary").replaceAll("\\r\\n", " ").replaceAll("\\n", " ")))
                .forEach(rowmap -> writer.writeRow(Arrays.stream(headers).map(h -> rowmap.get(h)).collect(Collectors.toList()).toArray(new String[0])));
        writer.close();
    }
}
