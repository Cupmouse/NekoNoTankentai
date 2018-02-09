package net.nekonium.explorer.server.handler.search;

import org.json.JSONArray;

interface SearchResultElement {

    JSONArray toJSONArray();

    int getPriority();

}
