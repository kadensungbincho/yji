MY_BUCKET=yji

aws configure
aws s3 cp data s3://$MY_BUCKET/data/ --recursive
