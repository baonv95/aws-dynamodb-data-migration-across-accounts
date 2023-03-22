const { DynamodbDatasource } = require('./datasource');

const dsSource = new DynamodbDatasource({
  profile: 'profile1'
})

const dsDestination = new DynamodbDatasource({
  profile: 'profile2'
})

const callbackTest = (arg)=>{
  console.log({arg}, 'callbacks');
}

const main = async ()=>{
  const callbacksChain = [];

  await dsSource.paginatedQueryWithCallback({
    tableName: 'Machine-Cleaning-Master',
    keyConditionExpression: 'PK = :PK AND begins_with ( SK, :SK )',
    expressionAttributeValues: {
      ':PK': 'PK',
      ':SK': 'SK'
    },
  }, callbackTest, 
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