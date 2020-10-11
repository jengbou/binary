#!/bin/sh
hcpbucket="hcp-openaccess"

# get list of subject id; only need to run when data set is updated with new subject
aws s3 ls --profile hcpopen s3://hcp-openaccess/HCP_1200/ |grep -o '[0-9]\{6\}' |awk '{print $1}' > datasets/hcp_1200_subjectid.txt

filelist="datasets/hcp_1200_subjectid.txt"

# subjects to be excluded due to missing files; put in ids separated by space 
exclIDs=(195041)

uip=4050
while read p; do
    if [[ "${exclIDs[@]}" =~ "${p}" ]]; then
        echo "Subject ID:" ${p} "is excluded"
    else
        echo "Submit spark batch job for subject ID:" ${p} "; using spark UI port:" ${uip}
        spark-submit --jars jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar --master spark://m5a2x0:7077 --conf spark.ui.port=${uip} --total-executor-cores 1 --executor-memory 1G examples/src/main/python/pipeline_hcpopen.py -b hcp-openaccess -n ${p} -s hcpopen -l /tmp/pipeline_hcpopen_${p}.log >& /tmp/${p}.log &
        sleep 1
        ((uip++))
    fi
done < $filelist
