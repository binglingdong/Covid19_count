create table cse532.neighborRelationship (zipcode1 VARCHAR(20),zipcode2 VARCHAR(20), zippop1 int, zippop2 int);

insert into cse532.neighborRelationship(zipcode1,zipcode2, zippop1, zippop2)

with groupByZipTable (zip, allPop) as ( --a total of 33120 valid zips
	select zip, sum(zpop) from cse532.zippop 
	where zip != 'ZIP'
	group by zip
),

zipTableWithShape(zip, pop, shape) as ( -- all 33120 zips with its shape
	select zip, allPop, shape from groupByZipTable inner join cse532.uszip 
	on groupByZipTable.zip = cse532.uszip.ZCTA5CE10
),

zipTablPopLessThanAvgWithShape(zip, pop, shape) as (
	select zip, pop, shape from zipTableWithShape 
	where pop < 12216 --12216 is avg
),

findZipLessThanAvgNeighbor(zip1, zip2, pop1, pop2) as(
	select table1.zip, table2.zip, table1.pop, table2.pop from zipTablPopLessThanAvgWithShape as table1 ,zipTableWithShape as table2
	where db2gse.ST_Intersects(table1.shape, table2.shape) = 1
	and  table1.zip != table2.zip
)
select * from findZipLessThanAvgNeighbor;



create table cse532.resultTable(zipcode VARCHAR(20), zpop int);

insert into cse532.resultTable(zipcode, zpop)

with groupByZipTable (zip, allPop) as ( --a total of 33120 valid zips
	select zip, sum(zpop) from cse532.zippop 
	where zip != 'ZIP'
	group by zip
),

zipTablPopLessThanAvg(zip, pop) as (
	select zip, allpop from groupByZipTable 
	where allpop >= 12216 --12216 is avg
)
select * from zipTablPopLessThanAvg;






