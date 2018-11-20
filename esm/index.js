import { doc as dynamoDb } from 'serverless-dynamodb-client';

class Index {
  table = null
  constructor(table) {
    this.table = table
  }
  get = async (entity, id) => {
    const { Item } = await dynamoDb
      .get({
        TableName: process.env.DYNAMODB_TABLE,
        Key: {
          id,
          entity,
        },
      })
      .promise();

    return Item ? Item.value : null;
  }
  put = async data => {
    if (!data.id) {
      throw new Error('data provided to dynamoDB.put should contain an id');
    }
    if (!data.entity) {
      throw new Error('data provided to dynamoDB.put should contain a partition');
    }
    if (!data.value) {
      throw new Error('data provided to dynamoDB.put should contain a value');
    }
    await dynamoDb
      .put({
        TableName: process.env.DYNAMODB_TABLE,
        Item: data,
      })
      .promise();
  }
  list = async entity => {
    const result = await dynamoDb.query({
      TableName : process.env.DYNAMODB_TABLE,
      KeyConditionExpression: "entity = :entity",
      ExpressionAttributeValues: {
        ":entity": entity,
      }
    }).promise()

    return result
  }
  entity = entity => ({
    get: id => this.get(entity, id),
    put: data => this.put({ ...data, entity }),
    list: () => this.list(entity)
  })
};

const dynamoDB = table => new Index(table)

export default dynamoDB;
