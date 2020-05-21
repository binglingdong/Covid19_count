DROP TABLE cse532.uszip;

create table cse532.uszip(
   ZCTA5CE10   varchar(5)
  ,GEOID10     varchar(5)
  ,CLASSFP10   varchar(2)
  ,MTFCC10     varchar(5)
  ,FUNCSTAT10  varchar(1)
  ,ALAND10 integer double
  ,AWATER10    double
  ,INTPTLAT10  varchar(11)
  ,INTPTLON10  varchar(12)
  ,shape       db2gse.st_multipolygon
  )
  ;


!db2se import_shape sample
-fileName         ./tar/tl_2019_us_zcta510.shp
-srsName          nad83_srs_1
-tableSchema      cse532
-tableName        uszip
-spatialColumn    shape
-typeSchema       db2gse
-typeName         st_multipolygon
-client           1
-messagesFile     zip.msg
;
 
!db2se register_spatial_column sample
-tableSchema      cse532
-tableName        uszip
-columnName       shape
-srsName          nad83_srs_1
;
 
 -- describe table cse532.uszip;
 