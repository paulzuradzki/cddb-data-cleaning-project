-- show all tables
select "name"
from sqlite_master
where type = 'table'
;

-- compare "artist" where value is different between source_df and clean_df
select
    t1.artist,
    t2.artist
from
    source_df t1
    inner join clean_df t2 on t1."index" = t2."index"
    and t1."artist" != t2."artist"
limit 10;
