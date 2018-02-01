


===================================================================
Q-7)

h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,
worksite:chararray,longitute:double,latitute:double);

applications = FOREACH h1b GENERATE case_status, year; 

grp1 = GROUP applications BY year;

totalapplication = FOREACH grp1 GENERATE group as year, COUNT(applications);

DUMP totalapplication;

o/p>>

hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/Pig/Q7NoOfApplcnt/*

2011	DENIED	8.119476	29130
2011	CERTIFIED	85.83175	307936
2011	WITHDRAWN	2.8165913	10105
2011	CERTIFIED-WITHDRAWN	3.2321813	11596
2012	DENIED	5.075949	21096
2012	CERTIFIED	84.856125	352668
2012	WITHDRAWN	2.5805628	10725
2012	CERTIFIED-WITHDRAWN	7.487362	31118
2013	CERTIFIED-WITHDRAWN	8.014222	35432
2013	WITHDRAWN	2.6214957	11590
2013	CERTIFIED	86.61816	382951
2013	DENIED	2.7461243	12141
2014	CERTIFIED-WITHDRAWN	6.998096	36350
2014	WITHDRAWN	3.086863	16034
2014	CERTIFIED	87.624245	455144
2014	DENIED	2.2907934	11899
2015	DENIED	1.765399	10923
2015	CERTIFIED	88.452255	547278
2015	WITHDRAWN	3.1443594	19455
2015	CERTIFIED-WITHDRAWN	6.6379843	41071
2016	CERTIFIED	87.93507	569646
2016	WITHDRAWN	3.3791137	21890
2016	CERTIFIED-WITHDRAWN	7.269494	47092
2016	DENIED	1.4163257	9175


===============================
Q-9)

h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


petitions = FOREACH h1b GENERATE case_status,employer_name; 

certified = FILTER petitions BY case_status == 'CERTIFIED';

withdrawn = FILTER petitions BY case_status == 'CERTIFIED-WITHDRAWN';

grp1 =  GROUP petitions BY employer_name;

grp2 = GROUP certified BY employer_name;

grp3 = GROUP withdrawn BY employer_name;


count1 = FOREACH grp1 GENERATE group as Emp_name, COUNT(petitions.case_status);

count2 = FOREACH grp2 GENERATE group as Emp_name, COUNT(certified);

count3 = FOREACH grp3 GENERATE group as Emp_name, COUNT(withdrawn);


joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0;

add = FOREACH joined GENERATE $0, $1, $3, $5;

total = FOREACH add GENERATE $0,$1,($2+$3);

success = FOREACH total GENERATE $0,$1,(float)($2)*100/(float)$1;

rate = FILTER success BY $1>=1000;

finalrate = FILTER rate BY $2>70;

successrate = LIMIT (ORDER finalrate BY $2 DESC)5;

DUMP successrate;

o/p>>
hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/Pig/Q9_EmployersWithTheNumberOfPetitions/*
INFOSYS LIMITED	130592	99.54055
ACCENTURE LLP	33447	99.39307
TATA CONSULTANCY SERVICES LIMITED	64726	99.337204
HCL AMERICA, INC.	22678	99.26801
RELIABLE SOFTWARE RESOURCES, INC.	1992	99.14658

=========================================================

h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


petitions = FOREACH h1b GENERATE case_status,job_title; 

certified = FILTER petitions BY case_status == 'CERTIFIED';

withdrawn = FILTER petitions BY case_status == 'CERTIFIED-WITHDRAWN';

grp1 =  GROUP petitions BY job_title;

grp2 = GROUP certified BY job_title;

grp3 = GROUP withdrawn BY job_title;


count1 = FOREACH grp1 GENERATE group as Job_title, COUNT(petitions);

count2 = FOREACH grp2 GENERATE group as Job_title, COUNT(certified);

count3 = FOREACH grp3 GENERATE group as Job_title, COUNT(withdrawn);


joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0;

total = FOREACH joined GENERATE $0,$1,($3+$5);

success = FOREACH total GENERATE $0,$1,(float)($2)*100/(float)$1;

successrate = FILTER success BY $1>=1000 AND $2>70;	

jobpositionssuccessrate = LIMIT (ORDER successrate BY $2 DESC)5;


DUMP jobpositionssuccessrate;

hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/Pig/Q10_JobPositionsWithTheNumberOfPetitions/*

COMPUTER PROGRAMMER / CONFIGURER 2	1276	100.0
ASSOCIATE CONSULTANT - US	4393	99.93171
SYSTEMS ENGINEER - US	10036	99.90036
TEST ANALYST - US	4958	99.818474
CONSULTANT - US	7426	99.81147































