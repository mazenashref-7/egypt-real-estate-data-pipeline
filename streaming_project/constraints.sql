ALTER TABLE silver.listing         DROP CONSTRAINT IF EXISTS fk_listing_location;
ALTER TABLE silver.listing         DROP CONSTRAINT IF EXISTS fk_listing_property;
ALTER TABLE silver.agent           DROP CONSTRAINT IF EXISTS fk_agent_broker;
ALTER TABLE silver.listing_amenity DROP CONSTRAINT IF EXISTS fk_amenity_listing;

ALTER TABLE silver.listing                 DROP CONSTRAINT IF EXISTS listing_pkey;
ALTER TABLE silver.location                DROP CONSTRAINT IF EXISTS location_pkey;
ALTER TABLE silver.property                DROP CONSTRAINT IF EXISTS property_pkey;
ALTER TABLE silver.agent                   DROP CONSTRAINT IF EXISTS agent_pkey;
ALTER TABLE silver.broker                  DROP CONSTRAINT IF EXISTS broker_pkey;
ALTER TABLE silver.listing_amenity         DROP CONSTRAINT IF EXISTS listing_amenity_pkey;
ALTER TABLE silver.egypt_locations         DROP CONSTRAINT IF EXISTS egypt_locations_pkey;
ALTER TABLE silver.egypt_inflation_monthly DROP CONSTRAINT IF EXISTS egypt_inflation_monthly_pkey;
ALTER TABLE silver.usd_egp_monthly         DROP CONSTRAINT IF EXISTS usd_egp_monthly_pkey;
ALTER TABLE silver.listings_stream         DROP CONSTRAINT IF EXISTS listings_stream_pkey;
ALTER TABLE silver.propertyfinder_listings  DROP CONSTRAINT IF EXISTS propertyfinder_listings_pkey;
ALTER TABLE silver.egypt_real_estate_listings DROP CONSTRAINT IF EXISTS egypt_real_estate_listings_pkey;

ALTER TABLE silver.listing                 ADD PRIMARY KEY (listing_id);
ALTER TABLE silver.location                ADD PRIMARY KEY (location_id);
ALTER TABLE silver.property                ADD PRIMARY KEY (property_id);
ALTER TABLE silver.agent                   ADD PRIMARY KEY (agent_id);
ALTER TABLE silver.broker                  ADD PRIMARY KEY (broker_id);
ALTER TABLE silver.listing_amenity         ADD PRIMARY KEY (listing_id, amenity);
ALTER TABLE silver.egypt_locations         ADD PRIMARY KEY (city_id);
ALTER TABLE silver.egypt_inflation_monthly ADD PRIMARY KEY (year_month);
ALTER TABLE silver.usd_egp_monthly         ADD PRIMARY KEY (year_month);
ALTER TABLE silver.listings_stream         ADD PRIMARY KEY (listing_id);
ALTER TABLE silver.propertyfinder_listings  ADD PRIMARY KEY (listing_id);
ALTER TABLE silver.egypt_real_estate_listings ADD PRIMARY KEY (url);

ALTER TABLE silver.listing ADD CONSTRAINT fk_listing_location
    FOREIGN KEY (location_id) REFERENCES silver.location(location_id);

ALTER TABLE silver.listing ADD CONSTRAINT fk_listing_property
    FOREIGN KEY (property_id) REFERENCES silver.property(property_id);

ALTER TABLE silver.agent ADD CONSTRAINT fk_agent_broker
    FOREIGN KEY (broker_id) REFERENCES silver.broker(broker_id);

ALTER TABLE silver.listing_amenity ADD CONSTRAINT fk_amenity_listing
    FOREIGN KEY (listing_id) REFERENCES silver.listing(listing_id);