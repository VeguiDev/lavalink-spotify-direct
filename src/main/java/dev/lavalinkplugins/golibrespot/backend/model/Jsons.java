package dev.lavalinkplugins.golibrespot.backend.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal hand-rolled JSON parser plus field accessors (zero dependencies).
 *
 * <p>The go-librespot v0.9.0 response shapes (docs/API_CONTRACT.md §7) are
 * simple fixed JSON objects; a tiny recursive-descent parser is all that is
 * needed. Unknown fields are tolerated by construction — the DTOs read only
 * the keys they know (compat rule, API_CONTRACT.md preamble). Numbers parse
 * to {@link Long} when integral, {@link Double} otherwise; strings support
 * the {@code \\uXXXX} escape and the standard escapes.</p>
 *
 * <p>Package-private: only the DTOs in this package parse JSON.</p>
 */
final class Jsons {

    private Jsons() {}

    /**
     * Parses a JSON document into {@code Map}/{@code List}/{@code String}/
     * {@code Long}/{@code Double}/{@code Boolean}/{@code null}.
     *
     * @throws IllegalArgumentException on malformed input
     */
    static Object parse(String text) {
        if (text == null) {
            return null;
        }
        Parser parser = new Parser(text);
        Object value = parser.value();
        parser.ws();
        if (!parser.atEnd()) {
            throw parser.error("trailing content");
        }
        return value;
    }

    /** Parses to a JSON object, or {@code null} for non-object / unparseable input. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String json) {
        Object value = parse(json);
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    /** {@code null} when the value is missing or not an object. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> asObject(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    /** String field, {@code ""} when missing/null/not a string (required fields degrade to empty). */
    static String str(Map<String, Object> o, String key) {
        String value = nullableStr(o, key);
        return value == null ? "" : value;
    }

    /** String field, {@code null} when missing/null/not a string (nullable fields). */
    static String nullableStr(Map<String, Object> o, String key) {
        Object value = o.get(key);
        return value instanceof String s ? s : null;
    }

    /** Integral field, {@code 0} when missing/null/not a number. */
    static long lng(Map<String, Object> o, String key) {
        Object value = o.get(key);
        return value instanceof Number n ? n.longValue() : 0L;
    }

    /** Integral field, {@code 0} when missing/null/not a number. */
    static int integer(Map<String, Object> o, String key) {
        Object value = o.get(key);
        return value instanceof Number n ? n.intValue() : 0;
    }

    /** Nullable integral field, {@code null} when missing/null/not a number. */
    static Integer nullableInt(Map<String, Object> o, String key) {
        Object value = o.get(key);
        return value instanceof Number n ? n.intValue() : null;
    }

    /** Boolean field, {@code false} when missing/null/not a boolean. */
    static boolean bool(Map<String, Object> o, String key) {
        Object value = o.get(key);
        return value instanceof Boolean b && b;
    }

    /** String array field, empty list when missing/null; non-string elements are skipped. */
    static List<String> strings(Map<String, Object> o, String key) {
        Object value = o.get(key);
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        List<String> out = new ArrayList<>(list.size());
        for (Object item : list) {
            if (item instanceof String s) {
                out.add(s);
            }
        }
        return List.copyOf(out);
    }

    // ------------------------------------------------------------------ parser

    private static final class Parser {

        private final String in;
        private int pos;

        Parser(String in) {
            this.in = in;
        }

        boolean atEnd() {
            return pos >= in.length();
        }

        void ws() {
            while (pos < in.length() && Character.isWhitespace(in.charAt(pos))) {
                pos++;
            }
        }

        Object value() {
            ws();
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            return switch (in.charAt(pos)) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        private Map<String, Object> object() {
            pos++; // '{'
            Map<String, Object> map = new LinkedHashMap<>();
            ws();
            if (!atEnd() && in.charAt(pos) == '}') {
                pos++;
                return map;
            }
            while (true) {
                ws();
                if (atEnd() || in.charAt(pos) != '"') {
                    throw error("expected string key");
                }
                String key = string();
                ws();
                if (atEnd() || in.charAt(pos) != ':') {
                    throw error("expected ':'");
                }
                pos++;
                map.put(key, value());
                ws();
                if (atEnd()) {
                    throw error("unterminated object");
                }
                char c = in.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == '}') {
                    pos++;
                    return map;
                }
                throw error("expected ',' or '}'");
            }
        }

        private List<Object> array() {
            pos++; // '['
            List<Object> list = new ArrayList<>();
            ws();
            if (!atEnd() && in.charAt(pos) == ']') {
                pos++;
                return list;
            }
            while (true) {
                list.add(value());
                ws();
                if (atEnd()) {
                    throw error("unterminated array");
                }
                char c = in.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == ']') {
                    pos++;
                    return list;
                }
                throw error("expected ',' or ']'");
            }
        }

        private String string() {
            pos++; // '"'
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (atEnd()) {
                    throw error("unterminated string");
                }
                char c = in.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (atEnd()) {
                    throw error("unterminated escape");
                }
                char e = in.charAt(pos++);
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (pos + 4 > in.length()) {
                            throw error("bad \\u escape");
                        }
                        sb.append((char) Integer.parseInt(in.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw error("bad escape '\\" + e + "'");
                }
            }
        }

        private Object number() {
            int start = pos;
            while (pos < in.length() && "-+0123456789.eE".indexOf(in.charAt(pos)) >= 0) {
                pos++;
            }
            String num = in.substring(start, pos);
            if (num.isEmpty()) {
                throw error("expected value");
            }
            if (num.indexOf('.') >= 0 || num.indexOf('e') >= 0 || num.indexOf('E') >= 0) {
                return Double.parseDouble(num);
            }
            return Long.parseLong(num);
        }

        private Object literal(String lit, Object value) {
            if (!in.startsWith(lit, pos)) {
                throw error("expected '" + lit + "'");
            }
            pos += lit.length();
            return value;
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at offset " + pos);
        }
    }
}
