package net.nekonium.explorer.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONUtil {

    private JSONUtil() {
    }

    public static boolean hasTyped(final JSONArray jsonArray, final int index, final Class<?> type) throws JSONException {
        return type.isInstance(jsonArray.get(index));
    }

    public static boolean hasString(final JSONArray jsonArray, final int index) throws JSONException {
        return jsonArray.get(index) instanceof String;
    }

    public static boolean hasNumber(final JSONArray jsonArray, final int index) throws JSONException {
        return jsonArray.get(index) instanceof Number;
    }

    public static boolean hasJSONObject(final JSONObject jsonObject, final String key) throws JSONException {
        return jsonObject.get(key) instanceof JSONObject;
    }

    public static boolean hasJSONArray(final JSONObject jsonObject, final String key) throws JSONException {
        return jsonObject.get(key) instanceof JSONArray;
    }

}
