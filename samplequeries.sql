
select s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord) 
    from (select name,bodyCount,coord from systems where population=0) as s1 
    join (select name,coord,population from systems where population>1) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,16);


select count(s1.name)
    from (select name,bodyCount,coord from systems where population=0) as s1 
    join (select name,coord from systems where population>1) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,16);


-- With spgist (small set)
--  count
-- --------
--  359455
-- (1 row)


-- Time: 2989.059 ms (00:02.989)

-- with gist (small set)
--  count
-- --------
--  359455
-- (1 row)


-- Time: 3473.360 ms (00:03.473)



--- explain analyse
select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord 
        from systems 
        inner join  (
            select bodies.system_id64
            from bodies 
            join rings 
            on rings.body_id64=bodies.body_id64 
            where rings.type='Icy'
            and bodies.parents='[{''Star'': 1}, {''Null'': 0}]'
            and bodies.distance_arv<3000
            ) as bodies on systems.id64=bodies.system_id64
        where population=0
        and bodyCount>50
        and st_3ddwithin(st_pointz(0, 0, 0),coord,2000)
        and st_3ddwithin(st_pointz(0, 0, 0),coord,2000)
        ) as s1 
    join (
        select name,coord,population from systems where population>1 and st_3ddwithin(st_pointz(0, 0, 0),coord,2000)) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;




select * 
from systems 
join bodies on systems.id64=bodies.system_id64 
join rings on rings.body_id64=bodies.body_id64 
where systems.name like 'Col 285 Sector OK-T b18-2%';




select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord 
        from systems 
        inner join  (
            select bodies.system_id64
            from bodies 
            join rings 
            on rings.body_id64=bodies.body_id64 
            where rings.type='Metallic'
            and bodies.distance_arv<3000
            ) as bodies on systems.id64=bodies.system_id64
        where population=0
        and bodyCount>50
        and st_3ddwithin(st_pointz(-500, 0, 500),coord,700)
        ) as s1 
    join (
        select name,coord,population from systems where population>1 and st_3ddwithin(st_pointz(0, 0, 0),coord,2000)) as s2 
    on s1.coord!=s2.coord 
    where st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;



select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord 
        from systems 
        inner join  (
            select bodies.system_id64
            from bodies 
            join rings 
            on rings.body_id64=bodies.body_id64 
            where rings.type='Icy'
            and bodies.distance_arv<3000
            ) as bodies on systems.id64=bodies.system_id64
        where population=0
        and st_3ddwithin(st_pointz(0, 0, 0),coord,5000)
        ) as s1 
    join (
        select name,coord,population,stations.services from systems
        inner join  (
            select stations.system_id64, stations.services
            from stations
            where stations.services like '%System Colonisation%'
            ) as stations on systems.id64=stations.system_id64 
        where population>1 
        and st_3ddwithin(st_pointz(0, 0, 0),coord,5000)
        ) as s2 
    on st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;




select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol,st_3ddistance(s1.coord,st_pointz(-9530,-910,19808)) as dcolonia
    from (
        select systems.name,bodyCount,coord 
        from systems 
        inner join  (
            select bodies.system_id64
            from bodies 
            inner join rings 
            on rings.body_id64=bodies.body_id64 
            where bodies.distance_arv<3000
            ) as bodies on systems.id64=bodies.system_id64
        where population=0
        and bodyCount>50
        and st_3ddwithin(st_pointz(0, 0, 0),coord,1000)
        ) as s1 
    where st_3ddwithin(s1.coord,st_pointz(-40.38 , 63.16 , 605.97),60))
    order by dcolonia asc;


select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord 
        from systems 
        inner join  (
            select bodies.system_id64
            from bodies 
            and bodies.subType = 'Rocky'
            ) as bodies on systems.id64=bodies.system_id64
        where population=0
        and st_3ddwithin(st_pointz(0, 0, 0),coord,1000)
        ) as s1 
    join (
        select name,coord,population,stations.services from systems
        inner join  (
            select stations.system_id64, stations.services
            from stations
            where stations.services like '%System Colonisation%'
            ) as stations on systems.id64=stations.system_id64 
        where population>1 
        and st_3ddwithin(st_pointz(0, 0, 0),coord,1000)
        ) as s2 
    on st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;







            and bodies.isLandable='t'


# near industrial colony col 359 sector hy-g b12-6
EXPLAIN select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord 
        from (select * from systems where st_3ddwithin(st_pointz(-15.06, -1.09,  386.19),coord,30)) as systems
        inner join  (
            select bodies.system_id64
            from bodies 
            where bodies.subType = 'Rocky'
            OR bodies.subType = 'HMC'
            ) as bodies on systems.id64=bodies.system_id64 
        where population=0
        ) as s1 
    join (
        select name,coord,population,stations.services from systems
        inner join  (
            select stations.system_id64, stations.services
            from stations
            where stations.services like '%System Colonisation%'
            ) as stations on systems.id64=stations.system_id64 
        where population>1 
        and st_3ddwithin(st_pointz(-15.06, -1.09,  386.19),coord,30)
        ) as s2 
    on st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;

EXPLAIN select * from 
(select distinct on (s1.name,s1.coord) s1.name as colony_name,s1.bodycount,s2.name as source_name,s2.population,st_3ddistance(s1.coord,s2.coord),st_3ddistance(s1.coord,st_pointz(0, 0, 0))as dsol
    from (
        select systems.name,bodyCount,coord,subtype
        from (select * from systems where st_3ddwithin(st_pointz(-15.06, -1.09,  386.19),coord,30)) as systems
        inner join  (
            select bodies.system_id64,subtype
            from bodies 
            where isLandable='t'
            ) as bodies on systems.id64=bodies.system_id64 
        where population=0
        and to_tsvector(bodies.subtype) @@ to_tsquery('Rocky')
        ) as s1 
    join (
        select name,coord,population,stations.services from systems
        inner join  (
            select stations.system_id64, stations.services
            from stations
            where stations.services like '%System Colonisation%'
            ) as stations on systems.id64=stations.system_id64 
        where population>1 
        and st_3ddwithin(st_pointz(-15.06, -1.09,  386.19),coord,30)
        ) as s2 
    on st_3ddwithin(s1.coord,s2.coord,15))
    order by dsol desc;





















select name,st_astext(coord) from systems where systems.name like '%colonia%';
