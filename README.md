# ecc

# Build
* `docker build -t ecc .`
# Run
* `docker run -e ECC_DURATION=1800 -dti ecc`
* For production: `docker run --name ecc_prod -dti -v /etc/localtime:/etc/localtime:ro  -v /var/log/ecc:/ecc_log ecc`
* For test: `docker run --name ecc_debug -ti -v /etc/localtime:/etc/localtime:ro -v /root/jichao/developing/ecc:/ecc ecc_test /bin/bash`



# Notes:
* You have to prepare instantclient\_18\_3
