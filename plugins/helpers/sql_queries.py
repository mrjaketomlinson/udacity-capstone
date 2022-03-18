# Drop table statements
yelp_users_drop = 'DROP TABLE IF EXISTS yelp_users;'
yelp_businesses_drop = 'DROP TABLE IF EXISTS yelp_businesses;'
yelp_reviews_drop = 'DROP TABLE IF EXISTS yelp_reviews;'
demographics_drop = 'DROP TABLE IF EXISTS demographics;'
tax_climate_drop = 'DROP TABLE IF EXISTS tax_climate;'
tax_and_yelp_business_drop = 'DROP TABLE IF EXISTS tax_and_yelp_business;'
demographic_and_yelp_business_drop = 'DROP TABLE IF EXISTS demographic_and_yelp_business;'

# Create table statements
yelp_users_create = ("""
    CREATE TABLE IF NOT EXISTS yelp_users (
        user_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        review_count int,
        yelping_since varchar,
        useful int,
        funny int,
        fans int,
        average_stars float,
        compliment_hot int,
        compliment_more int,
        compliment_profile int,
        compliment_cute int,
        compliment_list int,
        compliment_note int,
        compliment_plain int,
        compliment_cool int,
        compliment_funny int,
        compliment_writer int,
        compliment_photos int
    );
""")

yelp_businesses_create = ("""
    CREATE TABLE IF NOT EXISTS yelp_businesses (
        business_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        address varchar,
        city varchar,
        state varchar,
        postal_code varchar,
        latitude float,
        longitude float,
        stars float,
        review_count int,
        is_open int,
        hours_monday varchar,
        hours_tuesday varchar,
        hours_wednesday varchar,
        hours_thursday varchar,
        hours_friday varchar,
        hours_saturday varchar,
        hours_sunday varchar
    );
""")

yelp_reviews_create = ("""
    CREATE TABLE IF NOT EXISTS yelp_reviews (
        review_id varchar PRIMARY KEY,
        user_id varchar,
        business_id varchar,
        stars float,
        date varchar,
        text varchar(65535),
        useful int,
        funny int,
        cool int
    );
""")

demographics_create = ("""
    CREATE TABLE IF NOT EXISTS demographics (
        total_population int,
        female_population float,
        count int,
        foreign_born float,
        state_code varchar,
        average_household_size float,
        city varchar,
        race varchar,
        male_population float,
        median_age float,
        number_of_veterans float,
        state varchar
    );
""")

tax_climate_create = ("""
    CREATE TABLE IF NOT EXISTS tax_climate (
        state varchar NOT NULL,
        overall_rank int,
        corporate_tax_rank int,
        individual_income_tax_rank int,
        sales_tax_rank int,
        property_tax_rank int,
        unemployment_insurance_tax_rank int
    );
""")

tax_and_yelp_business_create = ("""
    CREATE TABLE IF NOT EXISTS tax_and_yelp_business (
        state varchar NOT NULL,
        overall_rank int,
        corporate_tax_rank int,
        individual_income_tax_rank int,
        sales_tax_rank int,
        property_tax_rank int,
        unemployment_insurance_tax_rank int,
        num_yelp_businesses int,
        num_open_yelp_businesses int,
        avg_yelp_review_count float,
        avg_yelp_review_rating float
    );
""")

demographic_and_yelp_business_create = ("""
    CREATE TABLE IF NOT EXISTS demographic_and_yelp_business (
        state varchar NOT NULL,
        state_code varchar NOT NULL,
        city varchar,
        median_age float,
        male_population int,
        female_population int,
        total_population int,
        number_of_veterans int,
        foreign_born int,
        average_household_size float,
        num_yelp_businesses int,
        num_open_yelp_businesses int,
        avg_yelp_review_count float,
        avg_yelp_review_rating float
    );
""")

tax_and_yelp_business_insert = ("""
    INSERT INTO tax_and_yelp_business
    SELECT 
        t.state AS state,
        MAX(t.overall_rank) AS overall_rank,
        MAX(t.corporate_tax_rank) AS corporate_tax_rank,
        MAX(t.individual_income_tax_rank) AS individual_income_tax_rank,
        MAX(t.sales_tax_rank) AS sales_tax_rank,
        MAX(t.property_tax_rank) AS property_tax_rank,
        MAX(t.unemployment_insurance_tax_rank) AS unemployment_insurance_tax_rank,
        COUNT(y.business_id) AS num_yelp_businesses,
        SUM(y.is_open) AS num_open_yelp_businesses,
        AVG(y.review_count) AS avg_yelp_review_count,
        AVG(y.stars) AS avg_yelp_review_rating
    FROM yelp_businesses y
        JOIN demographics d ON y.state = d.state_code
        JOIN tax_climate t ON t.state = d.state
    GROUP BY t.state
    ORDER BY t.state
""")

demographic_and_yelp_business_insert = ("""
    INSERT INTO demographic_and_yelp_business
    SELECT
        d.state AS state,
        d.city AS city,
        AVG(d.median_age) AS median_age,
        MAX(d.male_population) AS male_population,
        MAX(d.female_population) AS female_population,
        MAX(d.total_population) AS total_population,
        MAX(d.number_of_veterans) AS number_of_veterans,
        MAX(d.foreign_born) AS foreign_born,
        AVG(d.average_household_size) AS average_household_size,
        COUNT(y.business_id) AS num_yelp_businesses,
        SUM(y.is_open) AS num_open_yelp_businesses,
        AVG(y.review_count) AS avg_yelp_review_count,
        AVG(y.stars) AS avg_yelp_review_rating
    FROM yelp_businesses y
        JOIN demographics d ON d.state_code = y.state AND d.city = y.city
    GROUP BY d.state, d.city
    ORDER BY d.state, d.city
""")

# Query lists
table_create_statements = [yelp_businesses_create, yelp_reviews_create, yelp_users_create, demographics_create, tax_climate_create, tax_and_yelp_business_create, demographic_and_yelp_business_create]
table_drop_statements = [yelp_businesses_drop, yelp_reviews_drop, yelp_users_drop, demographics_drop, tax_climate_drop, tax_and_yelp_business_drop, demographic_and_yelp_business_drop]
table_insert_statements = [tax_and_yelp_business_insert, demographic_and_yelp_business_insert]

# Data quality checks
dq_checks = [
    {'sql': 'SELECT count(1) FROM yelp_users WHERE name IS NULL', 'expected_result': 0},
    {'sql': 'SELECT count(1) FROM yelp_businesses WHERE name IS NULL', 'expected_result': 0},
    {'sql': 'SELECT count(1) FROM yelp_reviews WHERE stars IS NULL', 'expected_result': 0},
    {'sql': 'SELECT count(1) FROM tax_climate WHERE state IS NULL', 'expected_result': 0}
]