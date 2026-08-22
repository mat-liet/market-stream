-- Seed the two v1 markets (design doc 24.2).
--
-- Kraken WS v2 uses the canonical "BTC/USD" form on the wire, so exchange_symbol
-- and canonical_symbol coincide today. They are kept as separate columns because
-- they will not coincide for a second exchange — that mapping is exactly what
-- phase 5 needs and what keeps Kraken specifics out of the canonical layer.

INSERT INTO instrument (
    exchange, canonical_symbol, exchange_symbol,
    base_asset, quote_asset,
    tick_size, lot_size,
    price_precision, quantity_precision,
    book_depth, enabled
) VALUES
    ('KRAKEN', 'BTC/USD', 'BTC/USD', 'BTC', 'USD', 0.1,  0.00000001, 1, 8, 10, TRUE),
    ('KRAKEN', 'ETH/USD', 'ETH/USD', 'ETH', 'USD', 0.01, 0.00000001, 2, 8, 10, TRUE);

INSERT INTO processor_version (processor_version, schema_version, notes)
VALUES ('0.1.0-SNAPSHOT', 1, 'Phase 1 walking skeleton');
