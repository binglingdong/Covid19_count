create procedure find_stddev(out stddev double)
  language sql
  begin
	declare sqlstate char(5) default '00000';
    	declare p_xsum double;
	declare p_count integer;
	declare p_sumOfxSquare double;
	declare p_sal double;
    	declare c cursor for select salary from employee;

	set p_xsum = 0;
	set p_count = 0;
	set p_sumOfxSquare = 0;


    	open c;
    	fetch from c into p_sal;
	while(sqlstate = '00000') do
	  set p_count = p_count +1;
	  set p_xsum = p_xsum + p_sal;
	  set p_sumOfxSquare = p_sumOfxSquare + p_sal*p_sal;
	  fetch from c into p_sal;
	end while;
	close c;
	set stddev = sqrt(p_sumOfxSquare/p_count - (p_xsum/p_count * p_xsum/p_count));
  end@
	
	