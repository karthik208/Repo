#!/bin/bash
#Shebang to represent this is bash shell based script
#bash ~/sfm_insuredata.sh https://s3.amazonaws.com/ins.data//insurance/insuranceinfo.csv
#debugger to understand the step by step execution of this script for learning, testing, debugging of the code
#prints the command along with the output
#set -x

<<steps
1. Formal way of writing shell scripts (shebang,comments, debugging, placeholder $#, $?, $0, $1, $2 ..)
2. Edgenode/Ingestionnode - 
Preparation of Landing pad/Staging area, archival path
Ingestion of data from some sources (filesystem/cloud storage sources)
Validation & Renaming of data (staging)
Rejection if data is not valid
Cleansing of trailer (which data is not needed for our consumption)
3. Hadoop Datalake (HDFS)
Preparation of HDFS Datalake path
Validation of the Datalake path
Ingestion of LFS feed to Datalake
Trigger/Indicator file creation
4. Archival of lfs data after compression
steps

#comments
######################################################
#Created by : hduser
#Created on : 24/04/24
#modified by: vadi
#Modified on : 16/12/23
#usage: bash sfm_insuredata.sh https://storage.googleapis.com/ins-gcs-data/srcdata/insuranceinfo.csv
#description: Automation of Data ingestion pipeline using SFM process - used to automate the SFM (source (cloud s3) Insurance cust info) -> ingestion(pull)-> Edgenode Landing pad -> Staging(lfs)->data validate -> reject & notification ->push->hdfs(datalake)-> Archival -> availability validate (trigger file mechanism) ->processing/analysing (consumers))

######################################################


#Define some variables (local or environment)
dt=`date '+%Y%m%d%H'`
echo $dt

#placeholders
#$# total number of arguments we are passing to a shell script
#$0 will hold the script name
#$1 will hold 1st argument

#Conditional check before executing the script
#check whether the enough arguments are passed to execute the script
if [ $# -ne 1 ]
then
echo "`date` missing the expected arguments, usage: bash /home/hduser/sfm_insuredata.sh https://s3.amazonaws.com/ins.data/insurance/insuranceinfo.csv"
echo "`date` missing the expected arguments, usage: bash /home/hduser/sfm_insuredata.sh https://s3.amazonaws.com/ins.data/insurance/insuranceinfo.csv" > /tmp/sfm_${dt}.log
exit 10
fi

#Actual business logic starts here

#mkdir -p /tmp/clouddata
echo "`date` Script started running " > /tmp/sfm_${dt}.log

#PREPARATION WORK
#Ensure the Landing pad and staging path in lfs is available, if not create
#mkdir -p /tmp/clouddata

if [ -d /tmp/clouddata ]
then
echo "Landing Pad, (where I am going to receive (landing pad) is present"
echo "Landing Pad, (where I am going to receive (landing pad) and validate & clean the data (staging)) is present" >> /tmp/sfm_${dt}.log
else
mkdir /tmp/clouddata
chmod -R 770 /tmp/clouddata
echo "Landing Pad is not present, hence created"
echo "Landing Pad is not present, hence created" >> /tmp/sfm_${dt}.log
fi

#Ensure the archive path in lfs is available, if not create
#mkdir -p /tmp/clouddata/archive
if [ -d /tmp/clouddata/archive ]
then
echo "archival path exists"
echo "archival path exists" >> /tmp/sfm_${dt}.log
else
mkdir /tmp/clouddata/archive
chmod -R 770 /tmp/clouddata
echo "archive Path is not present, hence created"
echo "archive Path is not present, hence created" >> /tmp/sfm_${dt}.log 
fi

#exit

#INGESTION WORK
#Data ingestion from cloud storage external source system (AWS S3/GCS/BLOB) to LFS landing pad is starting (Staging data)
#If the source system is pushing the files rather we pull, then the below step "wget $1 -O /tmp/clouddata/creditcard_insurance" is not needed
echo "`date` started importing data from cloud" >> /tmp/sfm_${dt}.log
#We can use wget/scp/sftp/ftp/ndm/mft/curl
#Push - In the source system side, they may use scp to securely copy the file from their server to ours..
#scp insuranceinfo.csv insureuser@127.0.0.1:/tmp/clouddata/creditcard_insurance
echo "the cloud url we are pull data from is $1"
#cp insuranceinfo.csv /tmp/clouddata/creditcard_insurance

wget $1 -O /tmp/clouddata/creditcard_insurance
#cp /home/hduser/insuranceinfo.csv /tmp/clouddata/creditcard_insurance
#cp $1 /tmp/clouddata/creditcard_insurance
if [ $? -eq 0 ]
then
echo "`date` import of data from cloud is completed" #Standard Output
echo "`date` import of data from cloud is completed" >> /tmp/sfm_${dt}.log
else
echo "no data imported from cloud  or failed to import "
echo "`date` no data imported from cloud or failed to import" >> /tmp/sfm_${dt}.log
exit 1
fi

#exit

#VALIDATION WORK + REJECTION + CLEANSING + DATA MOVEMENT FROM EDGE NODE TO HDFS DATALAKE + ARCHIVAL
#Data validation starts at staging layer level
if [ -f /tmp/clouddata/creditcard_insurance ]
then
 echo "`date` file is present, proceeding further"
 echo "`date` file is present, proceeding further" >> /tmp/sfm_${dt}.log
 mv /tmp/clouddata/creditcard_insurance /tmp/clouddata/creditcard_insurance_${dt} #renaming the file by appending date portion to it

 #Source Data validation starts here
 trlcnt=`tail -1 /tmp/clouddata/creditcard_insurance_${dt} | awk -F'|' '{ print $2 }'` #Identify the trl value provided by the source system in the last line
 filecnt=`cat /tmp/clouddata/creditcard_insurance_${dt} | wc -l` #identify the total count of the file
#filecnt=`wc -l creditcard_insurance_2023121613 | awk -F' ' '{print $1}'` #other way
# filecnt=10
 echo "trailer count is $trlcnt"
 echo "file count is $filecnt"
 if [ $trlcnt -ne $filecnt ] 
 #compare both counts are matching, then continue the script further, else fail the script and move the data to reject folder and come out with exit 2
 then
  echo "`date` moving to reject, file have invalid data" >> /tmp/sfm_${dt}.log
  mkdir -p /tmp/clouddata/reject
  mv /tmp/clouddata/creditcard_insurance_${dt} /tmp/clouddata/reject/
  echo "sending mail to the production support team and source system with the summary of issue"
  exit 2
 fi
 #exit
 #CLEANSING WORK - OF TRAILER DATA
 echo "`date` Remove the trailer line in the file"
 echo "`date` Remove the trailer line in the file" >> /tmp/sfm_${dt}.log
 sed -i '$d' /tmp/clouddata/creditcard_insurance_${dt}

 #PREPARATION OF HDFS Datalake location
 #Copy the staging data to the Datalake starts here
 hadoop fs -mkdir -p /user/hduser/insurance_clouddata/

 echo "Check whether the above (hdfs datalake) dir is created in Hadoop"
 hadoop fs -test -d /user/hduser/insurance_clouddata
 if [ $? -eq 0 ]
 then
  echo "`date` hadoop directory is exists/created " >> /tmp/sfm_${dt}.log
 else  
  echo "`date`  hadoop directory is not exists/ failed to create the hadoop directory /user/hduser/clouddata " >> /tmp/sfm_${dt}.log
  exit 3
 fi

 hadoop fs -mkdir -p /user/hduser/insurance_clouddata/datadt=${dt}

 #COPY/INGESTION of feeds from LFS to HDFS datalake
 hadoop fs -D dfs.block.size=256m -copyFromLocal -f /tmp/clouddata/creditcard_insurance_${dt} /user/hduser/insurance_clouddata/datadt=${dt}
 if [ $? -eq 0 ]
 then
 #TRIGGER/INDICATOR file creation for consumers understanding about the availability of data in the Datalake
  hadoop fs -touchz /user/hduser/insurance_clouddata/_SUCCESS
  echo "Data copied to HDFS successfully"
  echo "`date` Data copied to HDFS successfully" >>  /tmp/sfm_${dt}.log
 else
  echo "Failed to copy data to HDFS `date` " >> /tmp/sfm_${dt}.log
  #no exit codes, because we want to do archival of this data
 fi

 #gsutil -cp /tmp/clouddata/creditcard_insurance_${dt} gs://inceptez-bucket-data/

#ARCHIVAL/BACKUP (COMPRESSION) - Post Script clean up activity for Archive

 echo "`date` moving to linux archive after compressing" >> /tmp/sfm_${dt}.log
 gzip /tmp/clouddata/creditcard_insurance_${dt}
 mv /tmp/clouddata/creditcard_insurance_${dt}.gz /tmp/clouddata/archive/
 echo "`date` Data ingestion Pipeline completed " 
 echo "`date` Data ingestion Pipeline completed " >> /tmp/sfm_${dt}.log
else
 echo "No data to process"
 echo "`date` No data to process" >>  /tmp/sfm_${dt}.log
 exit 4
fi

echo "completed the data movement to hadoop after validation is completed, we will see later data movement to other systems and usage of this data by hive and other tools later"

exit 0 #later we will comment this exit


