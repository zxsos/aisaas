package io.bangbnag.commons.utils;

public final class StringFormatter {
    private static final char PLACEHOLDER = '$';
    private static final char ESCAPE = '\\';

    private StringFormatter() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    public static String format(String formatString, Object... params) {
        if (formatString == null)
            throw new IllegalArgumentException("Format string cannot be null");

        if (params == null || params.length == 0)
            return formatString;

        StringBuilder sb = new StringBuilder(formatString.length());
        int paramIndex = 0;
        char prevChar = 0;

        for (int i = 0; i < formatString.length(); i++) {
            char currentChar = formatString.charAt(i);

            if (currentChar == PLACEHOLDER && prevChar == ESCAPE) {
                sb.setCharAt(sb.length() - 1, currentChar);
            } else if (currentChar == PLACEHOLDER && paramIndex < params.length) {
                sb.append(params[paramIndex++]);
            } else {
                sb.append(currentChar);
            }

            prevChar = currentChar;
        }

        return sb.toString();
    }
}
