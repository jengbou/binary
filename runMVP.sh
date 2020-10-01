#!/bin/sh
hcpbucket="hcp-openaccess"

# get list of subject id; only need to run when data set is updated with new subject
aws s3 ls --profile hcpopen s3://hcp-openaccess/HCP_1200/ |grep -o '[0-9]\{6\}' |awk '{print $1}' > datasets/hcp_1200_subjectid.txt

filelist="datasets/hcp_1200_subjectid.txt"

# subjects to be excluded due to missing files; put in ids separated by space 
exclIDs=(195041)

while read p; do
    if [[ "${exclIDs[@]}" =~ "${p}" ]]; then
        echo "Subject ID:" ${p} "is excluded"
    else
        echo "Submit spark batch job for subject ID:" ${p}
        spark-submit --jars jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar --master spark://m5al0:7077 --total-executor-cores 2 --executor-memory 1G examples/src/main/python/pipeline_hcpopen_mvp.py -b hcp-openaccess -n ${p} -s hcpopen >& /tmp/${p}.log &
        sleep 1
    fi
done < $filelist
