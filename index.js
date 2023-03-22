const { DynamodbDatasource } = require('./datasource');

const DYNAMODB_TABLE_NAME = 'Table-Master';

const dsSource = new DynamodbDatasource({
  profile: 'source-dev'
});

const dsDestination = new DynamodbDatasource({
  profile: 'source-dev'
});

const callbackTest = (arg)=>{
  console.log({nbItems: arg.length}, 'callbacks');
}

const main = async ()=>{
  const callbacksChain = [];


  // ----------------------------------------------------------------
  //  ++ Migrate query-able data - enable bellow ++
  // ----------------------------------------------------------------

  const nbItems = await dsSource.paginatedQueryWithCallbackExecution({
    tableName: DYNAMODB_TABLE_NAME,
    keyConditionExpression: 'PK = :PK AND begins_with ( SK, :SK )',
    expressionAttributeValues: {
      ':PK': 'PK',
      ':SK': 'SK_PREFIX#'
    },
  }, 
  // callbackTest,
  dsDestination.batchWriteItem.bind(dsDestination, DYNAMODB_TABLE_NAME), 
  callbacksChain
  )

  // ----------------------------------------------------------------
  //  -- Migrate query-able data - enable above --
  // ----------------------------------------------------------------

  // ----------------------------------------------------------------
  //  ++ Migrate All data enable bellow ++
  // ----------------------------------------------------------------

  // const nbItems = await dsSource.paginatedScanWithCallbackExecution({
  //   tableName: DYNAMODB_TABLE_NAME,
  // }, 
  // callbackTest,
  // // dsDestination.batchWriteItem.bind(dsDestination, DYNAMODB_TABLE_NAME), 
  // callbacksChain
  // )

  // ----------------------------------------------------------------
  //  -- Migrate All data enable above --
  // ----------------------------------------------------------------

  // ----------------------------------------------------------------
  //  Wait till all writing jobs complete
  // ----------------------------------------------------------------

  await Promise.all(callbacksChain);

  console.info(`Number of items processed: ${nbItems}`)
}

main()
  .then(() => {
    console.log({ msg: 'Done' });
    process.exit(0);
  })
  .catch(e => {
    console.error(e);
    process.exit(1);
  })