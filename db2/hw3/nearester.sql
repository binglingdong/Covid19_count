with tempTableTwo(FACILITYNAME,ADDRESS1, ADDRESS2, CITY, STATE,ZIPCODE, GEOLOCATION)as (

select table2.FACILITYNAME,table2.ADDRESS1, table2.ADDRESS2, table2.CITY, table2.STATE, table2.ZIPCODE, table2.GEOLOCATION from cse532.facility as table2 inner join
	( select FacilityID
  	from cse532.facilitycertification
  	where AttributeValue = 'Emergency Department'
	) as table1 on table1.FacilityID =table2.FacilityID
)
select
  FacilityName, ADDRESS1, CITY, STATE, ZIPCODE,
  cast(db2gse.st_distance(GEOLOCATION, db2gse.st_point(-72.993983,40.824369, 1), 'STATUTE MILE') as decimal(8,4)) as distanceInMiles, db2gse.st_astext(GEOLOCATION) as location
from tempTableTwo
order by distanceInMiles
limit 1;