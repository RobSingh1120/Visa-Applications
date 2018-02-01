--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
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

--o/p>>
--hduser@rs-HP-Pavilion-Notebook:~$ hadoop fs -cat /H1bProject/Pig/Q9_EmployersWithTheNumberOfPetitions/*
--INFOSYS LIMITED	130592	99.54055
--ACCENTURE LLP	33447	99.39307
--TATA CONSULTANCY SERVICES LIMITED	64726	99.337204
--HCL AMERICA, INC.	22678	99.26801
--RELIABLE SOFTWARE RESOURCES, INC.	1992	99.14658
