package io.bangbnag.commons.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class ObjectWithUniqueID<D> implements Serializable {

    private String uniqueId;
    private D object; // NOSONAR
    private Map<String, String> headers;

    public ObjectWithUniqueID(D object) {
        this.object = object;
    }

    public ObjectWithUniqueID(D object, String uniqueId) {
        this.object = object;
        this.uniqueId = uniqueId;
    }

    public String getUniqueId() {
        if (this.uniqueId != null) {
            return this.uniqueId;
        }

        String objectStr = Objects.toString(object, "");
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] messageDigest = md.digest(objectStr.getBytes(StandardCharsets.UTF_8));
            BigInteger number = new BigInteger(1, messageDigest);
            this.uniqueId = number.toString(16);
        } catch (Exception ex) {
            // Fallback to a simple hash if SHA-256 is unavailable
            this.uniqueId = Integer.toHexString(objectStr.hashCode());
        }

        return this.uniqueId;
    }
}
