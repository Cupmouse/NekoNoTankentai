package net.nekonium.explorer.server.handler.search;

import net.nekonium.explorer.AddressType;
import org.json.JSONArray;

public class SearchResultElementAddressHash implements SearchResultElement {

    private final AddressType addressType;
    private final String addressHash;
    private final String alias;

    SearchResultElementAddressHash(AddressType addressType, String addressHash, String alias) {
        this.addressType = addressType;
        this.addressHash = addressHash;
        this.alias = alias;
    }

    @Override
    public JSONArray toJSONArray() {
        final JSONArray jsonArray = new JSONArray();

        jsonArray.put(ResultElementType.ADDRESS_HASH.toString());
        jsonArray.put(addressType.toString());
        jsonArray.put(addressHash);
        jsonArray.put(alias);

        return jsonArray;
    }

    @Override
    public int getPriority() {
        return 10000;
    }
}
