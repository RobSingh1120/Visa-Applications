Q-2A)
INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2011' group by worksite,year,case_status order by jobs desc limit 1;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2012' group by worksite,year,case_status order by jobs desc limit 1;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2013' group by worksite,year,case_status order by jobs desc limit 1;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2014' group by worksite,year,case_status order by jobs desc limit 1;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2015' group by worksite,year,case_status order by jobs desc limit 1;


INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2AmostDataEngJobs' row format delimited fields terminated by ','
select worksite, case_status,count(job_title) as jobs from h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='2016' group by worksite,year,case_status order by jobs desc limit 1;

--o/p>>
--hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/hive/Q2AmostDataEngJobs/*
--SEATTLE, WASHINGTON,CERTIFIED,121


Q-2B)

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2011' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2012' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2013' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2014' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2015' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q2BTop5Location' row format delimited fields terminated by ','
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2016' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

--0/p>>
--hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/hive/Q2BTop5Location/*
--2016,NEW YORK, NEW YORK,34639
--2016,SAN FRANCISCO, CALIFORNIA,13836
--2016,HOUSTON, TEXAS,13655
--2016,ATLANTA, GEORGIA,11678
--2016,CHICAGO, ILLINOIS,11064






Q-3)
INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q3MdataScPosition' row format delimited fields terminated by ','
select soc_name, count(*) as dscount from h1b.h1b_final where job_title like '%DATA SCIENTIST%' and case_status='CERTIFIED' group by soc_name order by dscount desc limit 1;
--o/p>>
--hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/hive/Q3MdataScPosition/*
--STATISTICIANS,572





Q-5 A)  

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2011' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2011	31799
--SOFTWARE ENGINEER	2011	12763
--COMPUTER PROGRAMMER	2011	8998
--SYSTEMS ANALYST	2011	8644
--BUSINESS ANALYST	2011	3891
--COMPUTER SYSTEMS ANALYST	2011	3698
--ASSISTANT PROFESSOR	2011	3467
--PHYSICAL THERAPIST	2011	3377
--SENIOR SOFTWARE ENGINEER	2011	2935
--SENIOR CONSULTANT	2011	2798

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2012' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2012	33066
--SOFTWARE ENGINEER	2012	14437
--COMPUTER PROGRAMMER	2012	9629
--SYSTEMS ANALYST	2012	9296
--BUSINESS ANALYST	2012	4752
--COMPUTER SYSTEMS ANALYST	2012	4706
--SOFTWARE DEVELOPER	2012	3895
--PHYSICAL THERAPIST	2012	3871
--ASSISTANT PROFESSOR	2012	3801
--SENIOR CONSULTANT	2012	3737

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2013' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2013	33880
--SOFTWARE ENGINEER	2013	15680
--COMPUTER PROGRAMMER	2013	11271
--SYSTEMS ANALYST	2013	8714
--TECHNOLOGY LEAD - US	2013	7853
--TECHNOLOGY ANALYST - US	2013	7683
--BUSINESS ANALYST	2013	5716
--COMPUTER SYSTEMS ANALYST	2013	5043
--SOFTWARE DEVELOPER	2013	5026
--SENIOR CONSULTANT	2013	4326


INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2014' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2014	43114
--SOFTWARE ENGINEER	2014	20500
--COMPUTER PROGRAMMER	2014	14950
--SYSTEMS ANALYST	2014	10194
--SOFTWARE DEVELOPER	2014	7337
--BUSINESS ANALYST	2014	7302
--COMPUTER SYSTEMS ANALYST	2014	6821
--TECHNOLOGY LEAD - US	2014	5057
--TECHNOLOGY ANALYST - US	2014	4913
--SENIOR CONSULTANT	2014	4898

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2015' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2015	53436
--SOFTWARE ENGINEER	2015	27259
--COMPUTER PROGRAMMER	2015	14054
--SYSTEMS ANALYST	        2015	12803
--SOFTWARE DEVELOPER	2015	10441
--BUSINESS ANALYST	2015	8853
--TECHNOLOGY LEAD - US	2015	8242
--COMPUTER SYSTEMS ANALYST	2015	7918
--TECHNOLOGY ANALYST - US	2015	7014
--SENIOR SOFTWARE ENGINEER	2015	6013

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5A_MostPopularJobPositionAllApplications' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2016' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2016	53743
--SOFTWARE ENGINEER	2016	30668
--SOFTWARE DEVELOPER	2016	14041
--SYSTEMS ANALYST	2016	12314
--COMPUTER PROGRAMMER	2016	11668
--BUSINESS ANALYST	2016	9167
--COMPUTER SYSTEMS ANALYST	2016	6900
--SENIOR SOFTWARE ENGINEER	2016	6439
--DEVELOPER	2016	6084
--TECHNOLOGY LEAD - US	2016	5410


===============================================
Q-5 B)

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2011' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2011	28806
--SOFTWARE ENGINEER	2011	11224
--COMPUTER PROGRAMMER	2011	8038
--SYSTEMS ANALYST	2011	7850
--BUSINESS ANALYST	2011	3444
--COMPUTER SYSTEMS ANALYST	2011	3152
--ASSISTANT PROFESSOR	2011	3050
--PHYSICAL THERAPIST	2011	2911
--SENIOR SOFTWARE ENGINEER	2011	2595
--SENIOR CONSULTANT	2011	2585

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2012' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2012	29226
--SOFTWARE ENGINEER	2012	12273
--COMPUTER PROGRAMMER	2012	8483
--SYSTEMS ANALYST	2012	8399
--BUSINESS ANALYST	2012	4144
--COMPUTER SYSTEMS ANALYST	2012	4084
--SENIOR CONSULTANT	2012	3420
--SOFTWARE DEVELOPER	2012	3290
--PHYSICAL THERAPIST	2012	3284
--ASSISTANT PROFESSOR	2012	3033

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2013' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2013	29906
--SOFTWARE ENGINEER	2013	12973
--COMPUTER PROGRAMMER	2013	10202
--SYSTEMS ANALYST	2013	7850
--TECHNOLOGY LEAD - US	2013	7809
--TECHNOLOGY ANALYST - US	2013	7641
--BUSINESS ANALYST	2013	4993
--COMPUTER SYSTEMS ANALYST	2013	4554
--SOFTWARE DEVELOPER	2013	4316
--SENIOR CONSULTANT	2013	3996

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2014' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2014	38625
--SOFTWARE ENGINEER	2014	17278
--COMPUTER PROGRAMMER	2014	13796
--SYSTEMS ANALYST	2014	9161
--BUSINESS ANALYST	2014	6529
--SOFTWARE DEVELOPER	2014	6473
--COMPUTER SYSTEMS ANALYST	2014	6204
--TECHNOLOGY LEAD - US	2014	5055
--TECHNOLOGY ANALYST - US	2014	4911
--SENIOR CONSULTANT	2014	4535

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2015' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
--job_title	year	total
--PROGRAMMER ANALYST	2015	48203
--SOFTWARE ENGINEER	2015	23352
--COMPUTER PROGRAMMER	2015	12971
--SYSTEMS ANALYST	2015	11498
--SOFTWARE DEVELOPER	2015	9343
--TECHNOLOGY LEAD - US	2015	8238
--BUSINESS ANALYST	2015	7919
--COMPUTER SYSTEMS ANALYST	2015	7234
--TECHNOLOGY ANALYST - US	2015	7009
--SENIOR SOFTWARE ENGINEER	2015	5324

INSERT OVERWRITE DIRECTORY '/H1bProject/hive/Q5B_MostPopularJobPositionCertifiedApl' row format delimited fields terminated by ','
select job_title, year,count(*) as total  from h1b_final where year = '2016' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;
-->
---job_title	year	total
--PROGRAMMER ANALYST	2016	47964
--SOFTWARE ENGINEER	2016	25890
--SOFTWARE DEVELOPER	2016	12474
--SYSTEMS ANALYST	2016	10986
--COMPUTER PROGRAMMER	2016	10528
--BUSINESS ANALYST	2016	8175
--COMPUTER SYSTEMS ANALYST	2016	6205
--DEVELOPER	2016	5912
--SENIOR SOFTWARE ENGINEER	2016	5630
--TECHNOLOGY LEAD - US	2016	5405






















