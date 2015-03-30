bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/../conf/tajo-tpcds-env.sh

if [ "$TPCDS_DATA_DIR" == "" ]; then
  echo "TPCDS_DATA_DIR" property is required. Please config conf/tajo-tpcds-env.sh
  exit 1
fi

mvn clean -Dtest=TpcDSRemoteTest test