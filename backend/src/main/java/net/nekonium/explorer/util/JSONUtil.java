package net.nekonium.explorer.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONUtil {

    private JSONUtil() {
    }

    public static boolean hasTyped(final JSONArray jsonArray, int index, Class<?> type) throws JSONException {
        return type.isInstance(jsonArray.get(index));
    }

    public static boolean hasString(final JSONArray jsonArray, int index) throws JSONException {
        return jsonArray.get(index) instanceof String;
    }

    public static boolean hasJSONObject(JSONObject jsonObject, String key) {
        return jsonObject.get(key) instanceof JSONObject;
    }

    public static boolean hasJSONArray(JSONObject jsonObject, String key) {
        return jsonObject.get(key) instanceof JSONArray;
    }
}
