
package jae.muzzin.nuforc_extend.kmeans;

import java.util.Objects;

/**
 *
 * @author Admin
 */
public class Row {
    public double[] data;
    public String id;

    public Row(double[] data, String id) {
        this.data = data;
        this.id = id;
    }

    public Row() {
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.id);
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
        final Row other = (Row) obj;
        return Objects.equals(this.id, other.id);
    }
    
}
