import { describe, it, expect } from 'vitest';
import { buildQueryCommandInput } from '../src/command';

describe('buildQueryCommandInput', () => {
  it('should build query with partition key only', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'user-123',
        pKeyType: 'S',
        pKeyProp: 'userId',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.TableName).toBe('Users');
    expect(result.KeyConditionExpression).toBe('#pk = :pk');
    expect(result.ExpressionAttributeNames).toEqual({ '#pk': 'userId' });
    expect(result.ExpressionAttributeValues).toEqual({ ':pk': 'user-123' });
  });

  it('should build query with partition and sort key (equals)', () => {
    const input = {
      tableName: 'Orders',
      queryRequest: {
        pKey: 'customer-123',
        pKeyType: 'S',
        pKeyProp: 'customerId',
        sKey: 'order-456',
        sKeyType: 'S',
        sKeyProp: 'orderId',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.KeyConditionExpression).toBe('#pk = :pk AND #sk = :sk');
    expect(result.ExpressionAttributeNames).toEqual({
      '#pk': 'customerId',
      '#sk': 'orderId',
    });
    expect(result.ExpressionAttributeValues).toEqual({
      ':pk': 'customer-123',
      ':sk': 'order-456',
    });
  });

  it('should build query with sort key comparator (greater than)', () => {
    const input = {
      tableName: 'Logs',
      queryRequest: {
        pKey: 'app-logs',
        pKeyType: 'S',
        pKeyProp: 'application',
        sKey: '2023-01-01',
        sKeyType: 'S',
        sKeyProp: 'timestamp',
        skComparator: '>',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.KeyConditionExpression).toBe('#pk = :pk AND #sk > :sk');
  });

  it('should build query with BETWEEN comparator', () => {
    const input = {
      tableName: 'Events',
      queryRequest: {
        pKey: 'event-type-1',
        pKeyType: 'S',
        pKeyProp: 'eventType',
        sKey: '2023-01-01',
        sKeyType: 'S',
        sKeyProp: 'eventDate',
        skValue2: '2023-12-31',
        skComparator: 'BETWEEN',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.KeyConditionExpression).toBe('#pk = :pk AND #sk BETWEEN :sk AND :skValue2');
    expect(result.ExpressionAttributeValues).toHaveProperty(':skValue2', '2023-12-31');
  });

  it('should build query with BEGINS_WITH comparator', () => {
    const input = {
      tableName: 'Files',
      queryRequest: {
        pKey: 'folder-1',
        pKeyType: 'S',
        pKeyProp: 'folderId',
        sKeyType: 'S',
        sKeyProp: 'fileName',
        skComparator: 'BEGINS_WITH',
        skValue2: 'doc',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.KeyConditionExpression).toBe('#pk = :pk AND begins_with(#sk, :skValue2)');
    expect(result.ExpressionAttributeValues).toEqual({
      ':pk': 'folder-1',
      ':skValue2': 'doc',
    });
    expect(result.ExpressionAttributeValues).not.toHaveProperty(':sk');
  });

  it('should include projection expression', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'user-123',
        pKeyType: 'S',
        pKeyProp: 'userId',
      },
      projectionExpression: 'name, email, #customAttr',
      extraExpAttributeNames: { '#customAttr': 'customAttribute' },
    };

    const result = buildQueryCommandInput(input);

    expect(result.ProjectionExpression).toBe('#name, email, #customAttr');
    expect(result.ExpressionAttributeNames).toMatchObject({
      '#pk': 'userId',
      '#customAttr': 'customAttribute',
    });
  });

  it('should include filter expression', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'user-123',
        pKeyType: 'S',
        pKeyProp: 'userId',
      },
      filterExpression: '#age > :minAge',
      extraExpAttributeNames: { '#age': 'age' },
      extraExpAttributeValues: { ':minAge': 18 },
    };

    const result = buildQueryCommandInput(input);

    expect(result.FilterExpression).toBe('#age > :minAge');
    expect(result.ExpressionAttributeNames).toMatchObject({ '#age': 'age' });
    expect(result.ExpressionAttributeValues).toMatchObject({ ':minAge': 18 });
  });

  it('should support pagination with lastEvaluatedKey', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'user-123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: { userId: 'user-123', sortKey: 'item-50' },
        limit: 20,
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.ExclusiveStartKey).toEqual({ userId: 'user-123', sortKey: 'item-50' });
    expect(result.Limit).toBe(20);
  });

  it('should support sorting with ScanIndexForward', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'user-123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sorting: 'DESC',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.ScanIndexForward).toBe(false);
  });

  it('should use index name when provided', () => {
    const input = {
      tableName: 'Users',
      queryRequest: {
        pKey: 'active',
        pKeyType: 'S',
        pKeyProp: 'status',
        indexName: 'StatusIndex',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.IndexName).toBe('StatusIndex');
  });

  it('should parse numeric partition key', () => {
    const input = {
      tableName: 'Orders',
      queryRequest: {
        pKey: '12345',
        pKeyType: 'N',
        pKeyProp: 'orderId',
      },
    };

    const result = buildQueryCommandInput(input);

    expect(result.ExpressionAttributeValues?.[':pk']).toBe(12345);
  });
});
