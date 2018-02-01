
--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:long,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);

--DESCRIBE h1b;
--h1b: {s_no: bytearray,case_status: chararray,employer_name: chararray,soc_name: chararray,job_title: chararray,full_time_position: chararray,prevailing_wage: long,year: chararray,worksite: chararray,longitute: double,latitute: double}


finalh1b = FOREACH h1b GENERATE case_status,job_title;

--DESCRIBE finalh1b;
--finalh1b: {case_status: chararray,job_title: chararray}


allgrouped = GROUP finalh1b BY job_title;

--DESCRIBE allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,job_title: chararray)}}


allcount = FOREACH allgrouped GENERATE group as job_title,COUNT(finalh1b.case_status) as totalapplicaion;

--DESCRIBE allcount;
--allcount: {job_title: chararray,totalapplicaion: long}



filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

--DESCRIBE filterh1b;
--filterh1b: {case_status: chararray,job_title: chararray}


successgrouped = GROUP filterh1b BY job_title;

-- DESCRIBE successgrouped;
--successgrouped: {group: chararray,filterh1b: {(case_status: chararray,job_title: chararray)}}


successcount = FOREACH successgrouped GENERATE group AS job_title,COUNT(filterh1b.case_status) as totalsuccess;

--DESCRIBE successcount;
--successcount: {job_title: chararray,totalsuccess: long}



joined = JOIN allcount BY $0,successcount BY $0;

--DESCRIBE joined;
--joined: {allcount::job_title: chararray,allcount::totalapplicaion: long,successcount::job_title: chararray,successcount::totalsuccess: long}


finalbag = FOREACH joined GENERATE $0,$1 as petitions, (($3*100)/$1) AS successrate;

--DESCRIBE finalbag;
--finalbag: {job_title: chararray,petitions: long,successrate: float}

											
filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;


finaloutput = ORDER filtersuccessrate BY $2 DESC;



--STORE finaloutput INTO '/home/hduser/Desktop/H1bProject/pig/Q10_JobPositionsWithTheNumberOfPetitions';

DUMP finaloutput;
