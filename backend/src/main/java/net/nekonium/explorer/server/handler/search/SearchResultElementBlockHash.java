package net.nekonium.explorer.server.handler.search;

import org.json.JSONArray;

class SearchResultElementBlockHash implements SearchResultElement {

    private final String blockHash;

    SearchResultElementBlockHash(final String blockHash) {
        this.blockHash = blockHash;
    }

    @Override
    public JSONArray toJSONArray() {
        final JSONArray jsonArray = new JSONArray();

        jsonArray.put(ResultElementType.BLOCK_HASH.toString());
        jsonArray.put(this.blockHash);

        return jsonArray;
    }

    @Override
    public int getPriority() {
        return 10;
    }
}
