#!/bin/sh
hcpbucket="hcp-openaccess"

# get list of subject id; only need to run when data set is updated with new subject
aws s3 ls --profile hcpopen s3://hcp-openaccess/HCP_1200/ |grep -o '[0-9]\{6\}' |awk '{print $1}' > datasets/hcp_1200_subjectid.txt

filelist="datasets/hcp_1200_subjectid.txt"

master="spark://m5a2x0:7077"
pyfile="examples/src/main/python/pipeline_hcpopen.py"
inbucket="hcp-openaccess"
outs3subdit="data/HCP_output_v1"
schema="hcpopen_v1"

# subjects to be excluded due to missing files; put in ids separated by space 
exclIDs=(195041)

uip=4050
while read p; do
    if [[ "${exclIDs[@]}" =~ "${p}" ]]; then
        echo "Subject ID:" ${p} "is excluded"
    else
        echo "Submit spark batch job for subject ID:" ${p} "; using spark UI port:" ${uip}
        spark-submit --jars jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar --master ${master} --conf spark.ui.port=${uip} --total-executor-cores 1 --executor-memory 1G ${pyfile} -b ${inbucket} -d ${outs3subdit} -n ${p} -s ${schema} -l /tmp/pipeline_hcpopen_${p}.log >& /tmp/${p}.log &
        sleep 1
        ((uip++))
    fi
done < $filelist
