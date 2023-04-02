package jae.muzzin.nuforc_extend;

import de.siegmar.fastcsv.reader.NamedCsvReader;
import de.siegmar.fastcsv.writer.CsvWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * @author Admin
 */
public class Nuforc_extend {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        final HashMap<String, Map<String, float[]>> geocoder = new HashMap<>();
        var georeader = NamedCsvReader.builder().build(new FileReader("uscities.csv"));
        georeader.stream()
                .peek(r -> geocoder.putIfAbsent(r.getField("state_id").toLowerCase(), new HashMap<>()))
                .forEach(r -> geocoder.get(r.getField("state_id").toLowerCase()).put(r.getField("city").toLowerCase(),
                        new float[]{Float.parseFloat(r.getField("lat")),Float.parseFloat(r.getField("lng"))}));
        georeader = NamedCsvReader.builder().build(new FileReader("canadacities.csv"));
        
        georeader.stream()
                .peek(r -> geocoder.putIfAbsent(r.getField("province_id").toLowerCase(), new HashMap<>()))
                .forEach(r -> geocoder.get(r.getField("province_id").toLowerCase()).put(r.getField("city").toLowerCase(),
                        new float[]{Float.parseFloat(r.getField("lat")),Float.parseFloat(r.getField("lng"))}));
        var reader = NamedCsvReader.builder().build(new FileReader("nuforc_reports.csv"));
        
        CsvWriter writer = CsvWriter.builder().build(new FileWriter("nuforc_latlong.csv"));
        String[] headers = new String[]{"level_0","index","text","date_time","report_link","city","state","country","shape","duration","summary","posted","latitude","longitude"}; 
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
                .filter(rowmap -> geocoder.get(rowmap.get("state").toLowerCase()).containsKey(rowmap.get("city").toLowerCase()))
                .peek(rowmap -> rowmap.put("latitude", ""+geocoder.get(rowmap.get("state").toLowerCase()).get(rowmap.get("city").toLowerCase())[0]))
                .peek(rowmap -> rowmap.put("longitude", ""+geocoder.get(rowmap.get("state").toLowerCase()).get(rowmap.get("city").toLowerCase())[1]))
                .peek(rowmap -> rowmap.remove("stats"))
                .peek(rm -> rm.put("level_0", ids.getAsInt()+""))
                .peek(rm -> rm.put("index", rm.get("level_0")))
                .forEach(rowmap -> writer.writeRow(Arrays.stream(headers).map(h -> rowmap.get(h)).collect(Collectors.toList()).toArray(new String[0])));
        writer.close();
    }
}
