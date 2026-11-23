DO $$
BEGIN
    IF to_regclass('silver.listing') IS NOT NULL THEN
        ALTER TABLE silver.listing
            DROP CONSTRAINT IF EXISTS fk_listing_location;
        ALTER TABLE silver.listing
            DROP CONSTRAINT IF EXISTS fk_listing_property;
    END IF;

    IF to_regclass('silver.agent') IS NOT NULL THEN
        ALTER TABLE silver.agent
            DROP CONSTRAINT IF EXISTS fk_agent_broker;
    END IF;

    IF to_regclass('silver.listing_amenity') IS NOT NULL THEN
        ALTER TABLE silver.listing_amenity
            DROP CONSTRAINT IF EXISTS fk_amenity_listing;
    END IF;
END
$$;
