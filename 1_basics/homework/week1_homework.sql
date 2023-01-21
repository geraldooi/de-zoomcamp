-- Question 3. Count records
-- How many trips were totally made on January 15?
-- Tip: Started and finished on 2019-01-15
select count(1)
from green_tripdata_201901
where lpep_pickup_datetime::date = '2019-01-15'
	and lpep_dropoff_datetime::date = '2019-01-15';

-- Question 4. Largest trip for each day
-- Which was the day with the largest trip distance?
-- Tip: Use the pick up time for your calculations.
select 
	lpep_pickup_datetime::date as pickup_time, 
	max(trip_distance) as largest_trip_distance
from green_tripdata_201901
group by 1
order by 2 desc
limit 1;

-- Question 5. The number of passengers
-- In 2019-01-01, how many trips had 2 and 3 passengers?
select 
	passenger_count, 
	count(1) as trip_count
from green_tripdata_201901
where lpep_pickup_datetime::date = '2019-01-01'
group by 1
having passenger_count in (2, 3);

-- Question 6. Largest tip
-- For the passengers picked up in the Astoria Zone, which was the drop off zone that has the largest tip?
select 
	coalesce(dropoff."Zone", 'Unknown') as dropoff_zone, 
	max(tip_amount) as largest_tip
from green_tripdata_201901 taxi
left join zone pickup on taxi."PULocationID" = pickup."LocationID"
left join zone dropoff on taxi."DOLocationID" = dropoff."LocationID"
where pickup."Zone" = 'Astoria'
group by 1
order by largest_tip desc
limit 1;