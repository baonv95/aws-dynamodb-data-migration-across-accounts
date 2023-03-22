const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  QueryCommand,
  BatchWriteCommand,
} = require('@aws-sdk/lib-dynamodb')

const QUERY_LIMIT = 1;

class DynamodbDatasource {
  constructor({profile}){
    const docClient = new DynamoDBClient({
      profile
    });

    this.dbClient = DynamoDBDocumentClient.from(docClient);
  }

  async paginatedQueryWithCallback({
    tableName, 
    keyConditionExpression, 
    expressionAttributeValues, 
    exclusiveStartKey
  }, callback,
  executionChain = [],
  page = 0) {
    const queryCommand = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      Limit: QUERY_LIMIT,
      ExclusiveStartKey: exclusiveStartKey
    });
    const {
      Items: items, 
      LastEvaluatedKey: lastEvaluatedKey
    } = await this.dbClient.send(queryCommand);
    
    console.info('--------------------------------\n');
    console.info('Found ' + items.length + ' items');
    console.table(items);
    console.info({lastEvaluatedKey}, 'paginatedQueryWithCallback');
  
    executionChain.push(callback(items));

    if(lastEvaluatedKey) { 
      page = page + 1;

      await this.paginatedQueryWithCallback({
        tableName,
        keyConditionExpression,
        expressionAttributeValues,
        exclusiveStartKey: lastEvaluatedKey
      }, callback, 
      executionChain
      )
    }
  }

  async batchWriteItem({
    items,
    tableName
  }) {
    const batchWriteCmd = new BatchWriteCommand({
      RequestItems: {
        [tableName]: items.map(obj => ({
          PutRequest: {
            Item: obj,
          },
        })),
      },
    });

    console.info('Item for batch Write');
    console.table(items);
    console.info('--------------------------------\n');

    await this.dbClient.send(batchWriteCmd);
    console.info('batchWriteItem - done');
  }
}

module.exports = {
  DynamodbDatasource
}