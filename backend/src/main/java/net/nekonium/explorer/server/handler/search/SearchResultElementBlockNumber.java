package net.nekonium.explorer.server.handler.search;

import org.json.JSONArray;

import java.math.BigInteger;

class SearchResultElementBlockNumber implements SearchResultElement {

    private final BigInteger blockNumber;

    SearchResultElementBlockNumber(BigInteger blockNumber) {
        this.blockNumber = blockNumber;
    }

    @Override
    public JSONArray toJSONArray() {
        final JSONArray jsonArray = new JSONArray();

        jsonArray.put(ResultElementType.BLOCK_NUMBER.toString());
        jsonArray.put(blockNumber.toString());

        return jsonArray;
    }

    @Override
    public int getPriority() {
        return 5000;
    }
}
