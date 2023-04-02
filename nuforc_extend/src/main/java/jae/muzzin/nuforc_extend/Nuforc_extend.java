package jae.muzzin.nuforc_extend;

import de.siegmar.fastcsv.reader.NamedCsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRow;
import java.io.FileNotFoundException;
import java.io.FileReader;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Admin
 */
public class Nuforc_extend {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        var reader = NamedCsvReader.builder().build(new FileReader("nuforc_reports.csv"));
        final Geocoder geocoder = new Geocoder();
        var cityGeoMap = new HashMap<String, float[]>();
        reader.stream()
                .map(row -> row.getField("city"))
                .filter(key -> !cityGeoMap.containsKey(key))
                .forEach(key -> {
                    try {
                        var req = new GeocoderRequestBuilder().setAddress(key).setLanguage("en").getGeocoderRequest();
                        var res = geocoder.geocode(req);
                        cityGeoMap.put(key, new float[]{res.getResults().get(0).getGeometry().getLocation().getLat().floatValue(), res.getResults().get(0).getGeometry().getLocation().getLng().floatValue()});
                    } catch (IOException ex) {
                        Logger.getLogger(Nuforc_extend.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });

    }
}
