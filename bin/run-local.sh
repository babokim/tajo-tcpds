bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/../conf/tajo-tpcds-env.sh

if [ "$TPCDS_DATA_DIR" == "" ]; then
  echo "TPCDS_DATA_DIR" property is required. Please config conf/tajo-tpcds-env.sh
  exit 1
fi

if [ "$TPCDS_DATA_GEN" == "true" ]; then
  if [ "$TPCDS_DATA_GENERATOR" == "" ]; then
    echo "TPCDS_DATA_GENERATOR" property is required. Please config conf/tajo-tpcds-env.sh
    exit 1;
  fi
  mkdir -p ${TPCDS_DATA_DIR}
  TABLE_NAMES="call_center,catalog_page,catalog_sales,customer,\
  customer_address,customer_demographics,date_dim,household_demographics,income_band,\
  inventory,item,promotion,reason,ship_mode,store,store_sales,time_dim,warehouse,\
  web_page,web_sales,web_site"

  CURRENT_PATH=`pwd`
  GENERATOR="${TPCDS_DATA_GENERATOR}"
  PARENTDIR="$(dirname "${GENERATOR}")"
  cd "${PARENTDIR}"
  for EACH_TABLE in $(echo "$TABLE_NAMES" | tr "," "\n")
  do
    TABLE_DATA_PATH=${TPCDS_DATA_DIR}/"${EACH_TABLE}"
    mkdir "${TABLE_DATA_PATH}"
    ${TPCDS_DATA_GENERATOR} -DIR "${TABLE_DATA_PATH}" -TABLE "${EACH_TABLE}" -SCALE 1
  done
  mkdir ${TPCDS_DATA_DIR}/catalog_returns
  mv ${TPCDS_DATA_DIR}/catalog_sales/catalog_returns* ${TPCDS_DATA_DIR}/catalog_returns/.

  mkdir ${TPCDS_DATA_DIR}/store_returns
  mv ${TPCDS_DATA_DIR}/store_sales/store_returns* ${TPCDS_DATA_DIR}/store_returns/.

  mkdir ${TPCDS_DATA_DIR}/web_returns
  mv ${TPCDS_DATA_DIR}/web_sales/web_returns* ${TPCDS_DATA_DIR}/web_returns/.

  cd ${CURRENT_PATH}
fi

mvn clean -Dtest=TpcDSLocalTest -DTPCDS_DATA_DIR=${TPCDS_DATA_DIR} test