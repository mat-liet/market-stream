package com.marketstream.schemas;

/**
 * The logical schema version stamped on every event's header.
 *
 * <p>Complements the Schema Registry's own version ID rather than replacing it: the
 * registry tracks wire compatibility, while this travels inside the payload and survives
 * being copied into ClickHouse, so a stored row still says which contract produced it.
 *
 * <p>Bump when the meaning of a field changes in a way consumers must notice, not on
 * every additive change — the registry's BACKWARD gate already covers those.
 */
public final class SchemaVersions {

    private SchemaVersions() {
    }

    public static final int CURRENT = 1;
}
