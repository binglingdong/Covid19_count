--drop procedure find_stddev;
create or replace procedure merge_zip(out std varchar(20))
  language sql
  begin
	declare sqlstate char(5) default '00000';
	declare zipcode1 varchar(20);
	declare zipcode2 varchar(20);
	declare lastZip varchar(20);
	declare p_population int;
	declare zippop1 int;
	declare zippop2 int;
    declare c1 cursor for select zipcode1 from cse532.neighborRelationship;
	declare c2 cursor for select zipcode2 from cse532.neighborRelationship;
	declare c3 cursor for select zippop1 from cse532.neighborRelationship;
	declare c4 cursor for select zippop2 from cse532.neighborRelationship;
    open c1;
    open c2;
    open c3;
    open c4;

    fetch from c1 into zipcode1;
	fetch from c2 into zipcode2;
	fetch from c3 into zippop1;
	fetch from c4 into zippop2;
	



	while(sqlstate = '00000') do
	  set lastZip = zipcode2;
	  set p_population = zippop2;
	  while (zipcode2 = lastZip) do
	    set p_population = p_population + zippop1;
	  	fetch from c1 into zipcode1;
	  	fetch from c2 into zipcode2;
		fetch from c3 into zippop1;
		fetch from c4 into zippop2;
	  end while;
	  insert into cse532.resultTable(zipcode, zpop) values (lastZip, p_population);
	  delete from cse532.neighborRelationship where zipcode1 = lastZip or zipcode2 = lastZip;
	  fetch from c1 into zipcode1;
	  fetch from c2 into zipcode2;
	  fetch from c3 into zippop1;
	  fetch from c4 into zippop2;

	end while;
	close c1;
  end@
	
	