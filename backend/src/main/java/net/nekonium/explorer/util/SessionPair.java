package net.nekonium.explorer.util;

import javax.websocket.Session;

public class SessionPair<P> {

    public final Session session;
    public final P param;

    public SessionPair(Session session, P param) {
        this.session = session;
        this.param = param;
    }

    @Override
    public int hashCode() {
        return session.hashCode();
    }
}
