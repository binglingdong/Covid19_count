
with EDZip(zipcode) as(	
	select substring(table2.Zipcode,1,5) from cse532.facility as table2 inner join
		( select FacilityID
  		  from cse532.facilitycertification
  		  where AttributeValue = 'Emergency Department'
		) as table1 on table1.FacilityID =table2.FacilityID
	group by substring(table2.Zipcode,1,5)
),
EDzipWithShape(zipcode, shape) as(
	select table2.zipcode, table1.shape from EDZip as table2 
	inner join cse532.uszip as table1 on table2.zipcode = substring(table1.ZCTA5CE10,1,5)
),

allZipandShapeNY(zipcode, shape) as(
	select table2.zipcode, table1.shape from (
		select substring(zipcode,1,5) as zipcode from cse532.facility group by substring(zipcode,1,5)) as table2 
	inner join cse532.uszip as table1 on table2.zipcode = substring(table1.ZCTA5CE10,1,5)
)

select zipcode from allZipandShapeNY where zipcode not in(   
		select allZip2.zipcode from EDzipWithShape as allZip1, allZipandShapeNY allZip2 	
		where db2gse.ST_Intersects(allZip1.shape, allZip2.shape) = 1 
	)
	group by zipcode
;
