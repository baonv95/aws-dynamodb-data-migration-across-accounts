const { DynamodbDatasource } = require('./datasource');

const DYNAMODB_TABLE_NAME = 'Table-Master';

const dsSource = new DynamodbDatasource({
  profile: 'source-dev'
});

const dsDestination = new DynamodbDatasource({
  profile: 'dest-dev'
});

const callbackTest = (arg)=>{
  console.log({arg}, 'callbacks');
}

const main = async ()=>{
  const callbacksChain = [];

  await dsSource.paginatedQueryWithCallback({
    tableName: DYNAMODB_TABLE_NAME,
    keyConditionExpression: 'PK = :PK AND begins_with ( SK, :SK )',
    expressionAttributeValues: {
      ':PK': 'PK',
      ':SK': 'SK_PREFIX'
    },
  }, 
  dsDestination.batchWriteItem.bind(dsDestination, DYNAMODB_TABLE_NAME), 
  callbacksChain
  )

  await Promise.all(callbacksChain);
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