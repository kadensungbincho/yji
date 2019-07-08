-- TABLE : reviews
SELECT * FROM reviews LIMIT 10;

SELECT count(*) FROM reviews;
-- 1,120,163

SELECT count(DISTINCT(listing_id)) FROM reviews;
-- 38677

SELECT DISTINCT(count(id)) FROM reviews;
-- 1,120,163

SELECT max(DATE), min(DATE) FROM reviews;
-- from 2009-03-12 to 2019-06-02


SELECT max(length(comments)) FROM reviews; -- LIMIT 200000;
-- 6,341

-- TABLE : neighbor
SELECT * FROM neighbor;

SELECT neighbourhood_group, count(DISTINCT(neighbourhood)) FROM neighbor GROUP BY 1;
-- 5 groups, each 32 ~ 53

-- TABLE : listings
SELECT * FROM listings LIMIT 10;

SELECT count(NAME)
	, count(host_response_rate)
	, count(host_acceptance_rate)
	, count(neighbourhood)

	, count(latitude)
	, count(bathrooms)
	, count(square_feet)
	, count(price)
	, count(calendar_updated)
FROM listings;


SELECT count(*) FROM listings;
-- 48,801

SELECT DISTINCT(count(host_id)) FROM listings;
-- 48,801

SELECT zipcode, count(*) FROM listings GROUP BY 1 ORDER BY 2 DESC;


-- TABLE : calendar

SELECT * FROM calendar LIMIT 100;
SELECT DATE, count(*) FROM calendar WHERE listing_id = 18764 GROUP BY 1 ORDER BY 1 DESC;
SELECT min(DATE), max(DATE) FROM calendar;




