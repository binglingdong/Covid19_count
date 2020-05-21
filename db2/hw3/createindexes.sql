drop index cse532.facilityidx;
drop index cse532.zipidx;

drop index cse532.hw3_index3;
drop index cse532.hw3_index4;


create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;

create index cse532.hw3_index3 on cse532.facilitycertification(FacilityID);
create index cse532.hw3_index4 on cse532.facility(zipcode);