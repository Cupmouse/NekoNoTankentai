package net.nekonium.explorer.server.handler.search;

import org.json.JSONArray;

class SearchResultElementTxHash implements SearchResultElement {

    private final String txHash;

    SearchResultElementTxHash(String txHash) {
        this.txHash = txHash;
    }

    @Override
    public JSONArray toJSONArray() {
        final JSONArray jsonArray = new JSONArray();

        jsonArray.put(ResultElementType.TX_HASH.toString());
        jsonArray.put(txHash);

        return jsonArray;
    }

    @Override
    public int getPriority() {
        return 1000;
    }
}
