
show topics;

-- this will show nothing
print 'USERS';

print 'USERS' from beginning;

print 'USERS' from beginning limit 2;

print 'USERS' from beginning interval 2 limit 2 ;

create stream users_stream (name VARCHAR, countrycode VARCHAR) WITH (KAFKA_TOPIC='USERS', VALUE_FORMAT='DELIMITED');

list streams;

-- nothing will get shown
select name, countrycode  from users_stream emit changes;

-- default to beginning of time
SET 'auto.offset.reset'='earliest';

-- now will see something
select name, countrycode  from users_stream emit changes;

-- stop after 4 records
select name, countrycode  from users_stream emit changes limit 4;

-- basic aggregate
select countrycode, count(*) from users_stream group by countrycode emit changes;

drop stream if exists users_stream delete topic;

describe userprofile;

--Streams from streams and functions
select firstname + ' ' 
+ ucase( lastname) 
+ ' from ' + countrycode 
+ ' has a rating of ' + cast(rating as varchar) + ' stars. ' 
+ case when rating < 2.5 then 'Poor'
       when rating between 2.5 and 4.2 then 'Good'
       else 'Excellent' 
   end as description
from userprofile emit changes;

--run a script which is present in current directory
run script 'user_profile_pretty.ksql';

--ksqlDB Tables
CREATE TABLE COUNTRYTABLE  (countrycode VARCHAR PRIMARY KEY, countryname VARCHAR) WITH (KAFKA_TOPIC='COUNTRY-CSV', VALUE_FORMAT='DELIMITED');

show tables;

describe COUNTRYTABLE;

describe COUNTRYTABLE extended;

--KSQL Joins
select up.firstname, up.lastname, up.countrycode, ct.countryname 
from USERPROFILE up 
left join COUNTRYTABLE ct on ct.countrycode=up.countrycode emit changes;

create stream up_joined as 
select up.firstname 
+ ' ' + ucase(up.lastname) 
+ ' from ' + ct.countryname
+ ' has a rating of ' + cast(rating as varchar) + ' stars.' as description 
, up.countrycode
from USERPROFILE up 
left join COUNTRYTABLE ct on ct.countrycode=up.countrycode;

select description from up_joined emit changes;