###############################################################################
# The path of TPC-DS test data set
export TPCDS_DATA_DIR=/Users/seungunchoe/tpc-data/tpcds/data_set
# export TPCDS_DATA_DIR=hdfs://127.0.0.1:9000/tpcds

# tpcds database name in Tajo
export DATABASE_NAME=tpcds

# The builtin results directory(src/test/resources/results) is for scale factor 1
# If you want to validate other scale factor, you should supply the valid result data.
# If this property is not set, validation will be skipped.
# export RESULTS_DIR=

###############################################################################
# Properties to run LocalTestCase.
# This test package doesn't supply test data set because test data set is too big.
# User should install TPC-DS tool kit or should generate test data set.
# If user already generate test data set, this property set false.
export TPCDS_DATA_GEN=false

# The path of TPC-DS data generator.
# If TPCDS_DATA_GEN set true, this property is required.
# TPCDS_DATA_GENERATOR generates test data set in TPCDS_DATA_DIR.
# export TPCDS_DATA_GENERATOR=/Users/babokim/Downloads/tpcds-kit-master/tools/dsdgen

###############################################################################
# Property to run on remote tajo cluster
# The client rpc address of Tajo Master.
#export TAJO_MASTER_CLIENT_ADDR=127.0.0.1:26002
