package net.nekonium.explorer.util;

import org.json.JSONArray;
import org.json.JSONException;

public class JSONUtil {

    private JSONUtil() {
    }

    public static boolean hasTyped(final JSONArray jsonArray, int index, Class<?> type) throws JSONException {
        return type.isInstance(jsonArray.get(index));
    }

    public static boolean hasString(final JSONArray jsonArray, int index) throws JSONException {
        return jsonArray.get(index) instanceof String;
    }
}
