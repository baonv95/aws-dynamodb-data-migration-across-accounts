const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
  BatchWriteCommand,
} = require('@aws-sdk/lib-dynamodb')

const BATCH_LIMIT = 25;

class DynamodbDatasource {
  constructor({profile}){
    const docClient = new DynamoDBClient({
      profile
    });

    this.dbClient = DynamoDBDocumentClient.from(docClient);
  }

  async paginatedScanWithCallbackExecution({
      tableName, 
      exclusiveStartKey,
      filterExpression,
      expressionAttributeValues,
    }, 
    callback,
    executionChain = [],
    page = 0,
    numberTouchedItems = 0
  ) {
    const queryCommand = new ScanCommand({
      TableName: tableName,
      Limit: BATCH_LIMIT,
      ExclusiveStartKey: exclusiveStartKey,
      FilterExpression: filterExpression,
      ExpressionAttributeValues: expressionAttributeValues
    });
    const {
      Items: items, 
      LastEvaluatedKey: lastEvaluatedKey
    } = await this.dbClient.send(queryCommand);

    numberTouchedItems = numberTouchedItems + items.length;
    
    console.info('--------------------------------\n');
    console.table(items);
    console.info('Found more ' + items.length + ' items in page ' + page + '. Total: ' + numberTouchedItems);
  
    executionChain.push(callback(items));

    if(lastEvaluatedKey) {
      page = page + 1;

      await this.paginatedScanWithCallbackExecution({
          tableName,
          exclusiveStartKey: lastEvaluatedKey,
          filterExpression,
          expressionAttributeValues
        }, 
        callback, 
        executionChain,
        page, 
        numberTouchedItems
      )
    }

    return numberTouchedItems;
  }

  async paginatedQueryWithCallbackExecution({
      tableName, 
      keyConditionExpression, 
      expressionAttributeValues, 
      exclusiveStartKey
    },
    callback,
    executionChain = [],
    page = 0,
    numberTouchedItems = 0
  ) {
    const queryCommand = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      Limit: BATCH_LIMIT,
      ExclusiveStartKey: exclusiveStartKey
    });
    const {
      Items: items, 
      LastEvaluatedKey: lastEvaluatedKey
    } = await this.dbClient.send(queryCommand);

    numberTouchedItems = numberTouchedItems + items.length;

    console.info('--------------------------------\n');
    console.table(items);
    console.info('Found more [' + items.length + '] items in page [' + page + ']. Total: [' + numberTouchedItems + ']');
  
    executionChain.push(callback(items));

    if(lastEvaluatedKey) { 
      page = page + 1;

      await this.paginatedQueryWithCallbackExecution({
          tableName,
          keyConditionExpression,
          expressionAttributeValues,
          exclusiveStartKey: lastEvaluatedKey
        },
        callback, 
        executionChain,
        page,
        numberTouchedItems
      )
    }

    return numberTouchedItems;
  }

  async batchWriteItem(tableName, items) {
    if(!items.length){
      return;
    }

    const batchWriteCmd = new BatchWriteCommand({
      RequestItems: {
        [tableName]: items.map(obj => ({
          PutRequest: {
            Item: {
              ...obj,
              // classification: 'GCD'
            },
          },
        })),
      },
    });

    console.info('\n--------------------------------');
    console.info(`[${items.length}] Items for batch Write`);
    console.table(items);
    console.info('--------------------------------\n');

    await this.dbClient.send(batchWriteCmd);
    console.info('batchWriteItem - done');
  }
}

module.exports = {
  DynamodbDatasource
}