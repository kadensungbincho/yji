#!/bin/sh
pushd /home/hadoop
n=0
e=5
until [ $n -ge $e ]
do
    # Yum update
    sudo yum -y update --skip-broken &&
    # Yum groupinstall for cmake installation - https://docs.aws.amazon.com/ko_kr/AWSEC2/latest/UserGuide/compile-software.html
    sudo yum groupinstall -y "Development Tools" &&
    # cmake installation
    wget --tries=5 https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.sh && mkdir cmake && sudo sh cmake-3.14.3-Linux-x86_64.sh --skip-license --prefix=/home/hadoop/cmake && sudo ln -s /home/hadoop/cmake/bin/cmake /usr/bin/cmake &&
    # Install python36
    sudo yum -y install python36 python36-devel python36-libs python36-pip python36-setuptools python36-test python36-tools &&
    # Install pip packages - default
    sudo pip-3.6 install boto3==1.9.145 h3==3.4.3 slackclient==1.3.1 pandas==0.23.4 numpy==1.14.5 scikit-learn==0.20.3 geopandas==0.5.0 scipy==1.2.1 spark-sklearn==0.3.0 geopy==1.19.0 matplotlib==3.0.3 pyarrow==0.10.0 &&
    # DB
    sudo pip-3.6 install sqlalchemy==1.3.3 pymysql==0.9.3 &&
    break
    n=$[$n+1]
    sleep 5
done
if [ $n -eq $e ]
then
    exit 1
fi
popd
exit 0
