ALTER TABLE IF EXISTS silver.listing_amenity DROP CONSTRAINT IF EXISTS fk_amenity_listing;
ALTER TABLE IF EXISTS silver.listing         DROP CONSTRAINT IF EXISTS fk_listing_location;
ALTER TABLE IF EXISTS silver.listing         DROP CONSTRAINT IF EXISTS fk_listing_property;
ALTER TABLE IF EXISTS silver.agent           DROP CONSTRAINT IF EXISTS fk_agent_broker;