const { DynamodbDatasource } = require('./datasource');

const dsSource = new DynamodbDatasource({
  profile: 'am-dev'
})

const dsDestination = new DynamodbDatasource({
  profile: 'pcmp-dev'
})

const callback = (arg)=>{
  console.log({arg}, 'callbacks');
}


const main = async ()=>{
  const callbackChain = [];

  await dsSource.paginatedQueryWithCallback({
    tableName: 'Machine-Cleaning-Master',
    keyConditionExpression: 'PK = :PK',
    expressionAttributeValues: {
      ':PK': 'CUSTOMER#52bac383-006a-46c7-80c9-36e572f57277',
    },
  }, callback, callbackChain)
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