package io.bangbang.commons.utils;

import jakarta.annotation.Nullable;
import org.springframework.expression.ParseException;
import reactor.core.publisher.Mono;

public class BooleanUtil {

    private static final Byte BYTE_0 = (byte) 0;

    private BooleanUtil() {
    }

    public static boolean safeValueOf(Object object) {

        switch (object) {
            case null -> {
                return false;
            }
            case Boolean b -> {
                return b;
            }
            case Byte b -> {
                return !BYTE_0.equals(object);
            }
            default -> {
            }
        }

        String value = object.toString();

        boolean returnValue = false;

        if (!value.isBlank()) {
            if ("yes".equalsIgnoreCase(value) || "y".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)
                    || "t".equalsIgnoreCase(value) || "on".equalsIgnoreCase(value)) {
                returnValue = true;
            } else {
                try {
                    int val = Integer.parseInt(value);
                    if (val != 0) {
                        returnValue = true;
                    }
                } catch (NumberFormatException nfe) {
                    return returnValue;
                }
            }
        }
        return returnValue;
    }

    @Nullable
    public static Boolean parse(Object object) {

        switch (object) {
            case null -> {
                return null;
            }
            case Boolean b -> {
                return b;
            }
            case Byte b -> {
                return !BYTE_0.equals(object);
            }
            default -> {
            }
        }

        String value = object.toString();

        if ("true".equalsIgnoreCase(value))
            return true;

        if ("false".equalsIgnoreCase(value))
            return false;

        throw new ParseException(0, object + " - Not a boolean value to parse");
    }

    public static Mono<Boolean> safeValueOfWithEmpty(Object b) {
        return Mono.just(safeValueOf(b)).filter(Boolean::booleanValue);
    }
}
