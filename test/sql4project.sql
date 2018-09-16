--continent table
create table username.continent(geom geometry(multipolygon,4326));
insert into username.continent(geom)
select st_union(s.geom) as geom
from publicdata.stateprov as s;


﻿--species with distribution geom
create table username.distribution (
spnr integer primary key,
  species character varying(80),
  eng_name character varying(80),
  tidx bigint,
  geom geometry(MultiPolygon,4326));

insert into username.distribution(spnr,species,eng_name,tidx,geom)
select s.nr,s.species,s.eng_name,s.tidx,s.geom
from publicdata.species as s
where s.geom is not null
order by s.nr;


--national boundary, type is multi-linestring
create table username.cntry_boundary (cntry_name character varying(80),geom geometry(multilinestring,4326));
insert into username.cntry_boundary(cntry_name,geom)
select s.cntry_name,st_multi(st_boundary(st_union(s.geom))) as geom
from publicdata.stateprov as s
group by s.cntry_name;



--function to get bbox polygon
CREATE OR REPLACE FUNCTION username.return_bbox_polygon(geom1 geometry,geom2 geometry)
RETURNS geometry AS
$BODY$
BEGIN
IF not  st_within(ST_setsrid(st_buffer(box2d($1),2),4326),$2)
THEN 
	RETURN st_setsrid(box2d(st_buffer(box2d($1),2)),4326); 
ELSE 
	RETURN st_setsrid(box2d(st_buffer(box2d(st_union(st_closestpoint(st_boundary($2),st_setsrid(box2d($1),4326)),
	st_setsrid(box2d($1),4326))),2)),4326);
END IF;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT
COST 10;
---------------------------------------------------

--distribution bbox+2degree
create table username.bbox (spnr integer,geom geometry(polygon,4326));
insert into username.bbox(spnr,geom)
select d.spnr,return_bbox_polygon(d.geom,c.geom)
from distribution as d,continent as c


--biggest bbox to decrease the run time
create table username.big_bbox as(
select st_union(st_intersection(e.geom,b.geom)) as geom
from bbox as b,publicdata.ecoregion as e
where st_intersects(e.geom,b.geom) and b.spnr in(
select b.spnr
from bbox as b
order by st_area(b.geom::geography) desc
limit 1))

--land_clip, type is multi-polygon
create table username.land_clip (spnr integer,geom geometry(multipolygon,4326));
insert into username.land_clip(spnr,geom)
select b.spnr,st_multi(st_union(st_intersection(bb.geom,b.geom)))
from big_bbox as bb,bbox as b
where st_intersects(b.geom,bb.geom)
group by b.spnr



--countary_boundary_clip, type is multi-linestring
create table username.cntryboundary_clip (spnr integer,geom geometry(multilinestring,4326));
insert into username.cntryboundary_clip(spnr,geom)
with a as (
select b.spnr,
case when st_geometrytype(st_intersection(c.geom,b.geom))='ST_GeometryCollection' then ST_CollectionExtract(st_intersection(c.geom,b.geom),2)
else st_intersection(c.geom,b.geom)
end 
from cntry_boundary as c,bbox as b
where st_intersects(b.geom,c.geom))
select a.spnr,st_multi(st_union(a.st_intersection)) as geom
from a
where st_geometrytype(a.st_intersection)!='ST_Point'
group by a.spnr;



--STATE boundary, type is multi-linestring
create table username.state_boundary (state_name character varying(80),geom geometry(multilinestring,4326));
insert into username.state_boundary(state_name,geom)
select s.admin_name,st_multi(st_boundary(s.geom)) as geom
from publicdata.stateprov as s;

--state boundary clip including all segments in each bbox
create table username.state_b_clip_temp as 
(select b.spnr,
(case when st_geometrytype(st_intersection(sb.geom,b.geom))='ST_GeometryCollection' then ST_CollectionExtract(st_intersection(sb.geom,b.geom),2)
else st_intersection(sb.geom,b.geom)
end)  as geom
from state_boundary as sb,bbox as b
where st_intersects(b.geom,sb.geom));



--stateboundary_clip,type is multilinestring. To avoid timeout situation
create table username.stateboundary_clip_line (spnr integer primary key,geom geometry(multilinestring,4326));

insert into username.stateboundary_clip_line(spnr,geom)
select s.spnr,st_multi(st_union(s.geom)) as geom
from state_b_clip_temp as s
where s.spnr <208300 -- the first 50 spnr
group by s.spnr;

insert into username.stateboundary_clip_line(spnr,geom)
select s.spnr,st_multi(st_union(s.geom)) as geom
from state_b_clip_temp as s
where s.spnr >= 208300 and s.spnr<217200 --51-100 spnr
group by s.spnr;

insert into username.stateboundary_clip_line(spnr,geom)
select s.spnr,st_multi(st_union(s.geom)) as geom
from state_b_clip_temp as s
where s.spnr >= 217200 and s.spnr<224000 --101-151 spnr
group by s.spnr;


insert into username.stateboundary_clip_line(spnr,geom)
select s.spnr,st_multi(st_union(s.geom)) as geom
from state_b_clip_temp as s
where s.spnr >=224000 --152-190 spnr
group by s.spnr;



------final table including species id(spnr),species name and all geometry 
create table username.final (
spnr integer,
eng_name character varying(80),
species_geom geometry(multipolygon,4326),
bbox_geom geometry(polygon,4326),
state_geom geometry(multipolygon,4326),
cntryboundary_geom geometry(multilinestring,4326),
stateboundary_geom geometry(multilinestring,4326)
);
insert into final(spnr,eng_name,species_geom,bbox_geom,state_geom,cntryboundary_geom,stateboundary_geom)
select d.spnr,d.eng_name,d.geom as species_geom, b.geom as bbox_geom,s.geom as state_geom,c.geom as cntryboundary_geom,sb.geom as stateboundary_geom
from distribution as d
	join bbox as b on b.spnr=d.spnr 
	left join land_clip as s on s.spnr=d.spnr
	left join cntryboundary_clip as c on c.spnr =d.spnr
	left join stateboundary_clip_line as sb on sb.spnr=d.spnr;



--delete the species not in the south america continent
DELETE 
FROM final 
  WHERE final.spnr in (
	select distinct d.spnr
	from distribution as d, continent as c
	where not st_intersects(d.geom,c.geom));



alter table username.final
add column id SERIAL PRIMARY KEY;

