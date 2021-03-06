package net.nekonium.explorer;

import net.nekonium.explorer.util.IllegalBlockchainStateException;
import net.nekonium.explorer.util.IllegalDatabaseStateException;
import net.nekonium.explorer.util.NonNullPair;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class AddressIdPoolManager {

    private static final int CACHE_INITIAL_CAPACITY = 1000;  // Initial capacity is big because there are NUMEROUS amount of addresses
    private LinkedHashMap<String, NonNullPair<AddressType, BigInteger>> addressIdCache;  // An unused address will be deleted from this map (un-caching)

    public AddressIdPoolManager() {
        this.addressIdCache = new AddressIdPool<>(CACHE_INITIAL_CAPACITY);  // Create a pool with initial capacity
    }

    private boolean isCached(String prefixedAddress) {
        return addressIdCache.containsKey(prefixedAddress);
    }

    private BigInteger getCached(String prefixedAddress) throws AddressPoolException {
        final NonNullPair<AddressType, BigInteger> cached = addressIdCache.get(prefixedAddress);    // The element accessed will be top on the list and be the least probable to un-cached

        if (cached == null) {
            throw new AddressPoolException("Requested but address " + prefixedAddress + " is not cached");
        }

        return cached.getB();
    }

    private BigInteger getCachedAssume(String prefixedAddress, AddressType addressType) throws AddressPoolException {
        final NonNullPair<AddressType, BigInteger> cached = addressIdCache.get(prefixedAddress);

        if (cached == null) {
            throw new AddressPoolException("Requested but address " + prefixedAddress + " is not cached");
        }
        if (cached.getA() != addressType) {
            throw new AddressPoolException("Detected cached account type inconsistency. Expected [" + addressType + "], but cached is [" + cached.getA() + "] : [" + prefixedAddress + "]");
        }

        return cached.getB();
    }

    private void cache(String prefixedAddress, AddressType addressType, BigInteger addressId) {
        this.addressIdCache.put(prefixedAddress, new NonNullPair<>(addressType, addressId));
    }

    private BigInteger getOrInsertDatabase(Connection connection, String prefixedAddress, AddressType addressType, boolean careType) throws SQLException, IllegalDatabaseStateException, AddressPoolException {
        /* Not cached, find address id */

        final PreparedStatement prpstmt = connection.prepareStatement("SELECT type, internal_id FROM addresses WHERE address = UNHEX(?)");
        prpstmt.setString(1, prefixedAddress.substring(2));

        final ResultSet resultSet = prpstmt.executeQuery();


        final BigInteger addressId;

        if (resultSet.next()) {
            // An address is recorded

            if (careType) {
                final AddressType recordedAddressType;

                try {
                    recordedAddressType = AddressType.valueOf(resultSet.getString(1));
                } catch (IllegalArgumentException e) {
                    throw new IllegalDatabaseStateException("Unknown address type", e);
                }

                if (recordedAddressType != addressType) {
                    throw new IllegalDatabaseStateException("Address type mismatch for [" + prefixedAddress + "], expected [" + addressType + "] got [" + recordedAddressType + "]");
                }
            }

            addressId = new BigInteger(resultSet.getString(2));

            prpstmt.close();
        } else {
            // An address is NOT recorded

            prpstmt.close();

            /* Insert an address into the database */

            final PreparedStatement prpstmti = connection.prepareStatement("INSERT INTO addresses VALUES (NULL, UNHEX(?) ,?, NULL, NULL)", RETURN_GENERATED_KEYS);
            prpstmti.setString(1, prefixedAddress.substring(2));
            prpstmti.setString(2, addressType.name());
            prpstmti.executeUpdate();

            final ResultSet generatedKeys = prpstmti.getGeneratedKeys();

            if (generatedKeys.next()) {
                addressId = new BigInteger(generatedKeys.getString(1));
            } else {
                throw new IllegalDatabaseStateException("Generated keys not returned");
            }

            prpstmti.close();

            cache(prefixedAddress, addressType, addressId);
        }

        return addressId;
    }

    /**
     * Look up address id by prefixed address (0x[40 digit hex]).<br>
     * If {@code careType} is {@code true}, {@link AddressType} of {@code prefixedAddress} must be the same as {@code addressType} and on the database,
     * and if {@code prefixedAddress} is first time to appear on the database, its type is recorded as {@code addressType}.<br>
     * If {@code careType} is {@code false}, it must NOT be the same, and if the address was not recorded on the database then it will inserted as {@code addressType} type.
     *
     * @param connection
     * @param prefixedAddress
     * @param addressType
     * @param careType Set true to throw an {@link IllegalDatabaseStateException} when {@code addressType} specified is not the same as on the database
     * @return address_id The address id of the address specified.
     * @throws IllegalBlockchainStateException If {@code addressType} and on the database are different (check is active when {@code careType} is {@code true}).
     */
    public BigInteger getOrInsertAddressId(Connection connection, String prefixedAddress, AddressType addressType, boolean careType) throws SQLException, IllegalDatabaseStateException, AddressPoolException {
        if (isCached(prefixedAddress)) {  // Find it from cache
            if (careType) {
                return getCachedAssume(prefixedAddress, addressType);   // Cached, return
            } else {
                return getCached(prefixedAddress);
            }
        }

        /* Not cached, find address id */

        return getOrInsertDatabase(connection, prefixedAddress, addressType, careType);
    }

    /**
     * Get the address id but no insert
     *
     * @param connection
     * @param prefixedAddress
     * @return
     * @throws SQLException
     * @throws IllegalDatabaseStateException If address is not recorded on the database
     */
    public BigInteger getAddressId(Connection connection, String prefixedAddress) throws SQLException, IllegalDatabaseStateException, AddressPoolException {
        if (isCached(prefixedAddress)) {
            return getCached(prefixedAddress);
        }

        final PreparedStatement prpstmt = connection.prepareStatement("SELECT internal_id FROM addresses WHERE address = UNHEX(?)");
        prpstmt.setString(1, prefixedAddress.substring(2));

        final ResultSet resultSet = prpstmt.executeQuery();

        if (!resultSet.next()) {
            throw new IllegalDatabaseStateException("An address [" + prefixedAddress + "] is not recorded!");
        }

        BigInteger addressId = new BigInteger(resultSet.getString(1));

        prpstmt.close();

        return addressId;
    }

    public class AddressIdPool<K, V> extends LinkedHashMap<K, V> {

        private final int poolSize;

        public AddressIdPool(int poolSize) {
            super(poolSize + 5, 1F, true);   // Load factor 1 means no extension of map until size() == capacity
            // Access order is true, the last accessed element will be the last element on the map
            this.poolSize = poolSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > poolSize;   // Removing unimportant cache when pool size exceeds
        }
    }
}
