MY_BUCKET=yji

aws configure
aws s3 cp data s3://$MY_BUCKET/data/ --recursive
aws s3 cp emr s3://$MY_BUCKET/emr/ --recursive
aws s3 cp docker_flask s3://$MY_BUCKET/docker_flask/ --recursive
