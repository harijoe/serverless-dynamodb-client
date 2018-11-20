import { doc as dynamoDb } from 'serverless-dynamodb-client';

class Index {
  table = null
  constructor(table) {
    this.table = table
  }
  get = async (type, id) => {
    const { Item } = await dynamoDb
      .get({
        TableName: this.table,
        Key: {
          id,
          type,
        },
      })
      .promise();

    return Item ? Item : null;
  }
  put = async data => {
    if (!data.id) {
      throw new Error('data provided to dynamoDB.put should contain an id');
    }
    if (!data.type) {
      throw new Error('data provided to dynamoDB.put should contain a partition');
    }
    if (!data.value) {
      throw new Error('data provided to dynamoDB.put should contain a value');
    }
    await dynamoDb
      .put({
        TableName: this.table,
        Item: data,
      })
      .promise();
  }
  // https://goo.gl/QwuKNr
  list = async (type, filters) => {
    const params = {
      TableName : this.table,
      KeyConditionExpression: "#type = :type",
      ExpressionAttributeNames: {
        '#type': 'type'
      },
      ExpressionAttributeValues: {
        ":type": type,
      }
    }

    if (filters) {
      const filterKeys = Object.keys(filters)

      if (filterKeys.length) {
        params.FilterExpression = filterKeys.map(key => `#${key} = :${key}`).join(' AND ')
      }

      filterKeys.forEach(key => {
        params.ExpressionAttributeNames[`#${key}`] = key
        params.ExpressionAttributeValues[`:${key}`] = filters[key]
      })
    }

    const result = await dynamoDb.query(params).promise()

    return result.Items
  }
  type = type => ({
    get: id => this.get(type, id),
    put: data => this.put({ ...data, type }),
    list: () => this.list(type)
  })
};

const dynamoDB = table => new Index(table)

export default dynamoDB;
