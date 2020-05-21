With groupMMEbyZip (buyerZip, MME) as
(select BUYER_ZIP,sum(MME)
   from cse532.dea_ny
   group by(buyer_Zip)
),
zipPop (zipcode, population) as
(select ZIP, sum(ZPOP)
 from cse532.zippop
 group by ZIP
 having sum(ZPOP) > 0
)
select groupMMEbyZip.buyerZip, groupMMEbyZip.MME, zipPop.population,
rank() over (order by groupMMEbyZip.MME/zipPop.population desc)as rank_MMEoverPop
from groupMMEbyZip inner join zipPop 
  on groupMMEbyZip.buyerZip = zipPop.zipcode
FETCH FIRST 5 ROWS ONLY;