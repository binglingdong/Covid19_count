With groupedMonthTable (date, monthlyCount) as
(select YEAR(TRANSACTION_DATE) ||
CASE 
    WHEN (  CAST( MONTH(TRANSACTION_DATE) AS INT)  < 10 ) 
         THEN '0'  || CAST( MONTH(TRANSACTION_DATE) AS INT)
    ELSE CAST ( MONTH(TRANSACTION_DATE) AS CHAR(2) )
END 
   ,sum(DOSAGE_UNIT)
   from cse532.dea_ny
   group by(year(TRANSACTION_DATE),month(TRANSACTION_DATE))
)
select date, monthlyCount, avg(monthlyCount) over
 (order by date rows between 1 PRECEDING and 1 FOLLOWING) as smoothCount2
from groupedMonthTable;

