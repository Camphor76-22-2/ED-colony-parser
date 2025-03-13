
select s1.name as colony_name,s1.bodycount,s2.name as source_name,st_3ddistance(s1.coord,s2.coord) 
    from (select name,bodyCount,coord from systems where population=0) as s1 
    join (select name,coord from systems where population>1) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,16);


select count(s1.name)
    from (select name,bodyCount,coord from systems where population=0) as s1 
    join (select name,coord from systems where population>1) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,16);