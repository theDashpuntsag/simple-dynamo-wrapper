import { describe, it, expect } from 'vitest';
import { buildPutCommandInput } from '../src/command';

describe('buildPutCommandInput', () => {
  it('should build basic PutCommand without condition', () => {
    const input = {
      tableName: 'Users',
      item: { userId: '123', name: 'John Doe', email: 'john@example.com' },
    };

    const result = buildPutCommandInput(input);

    expect(result).toEqual({
      TableName: 'Users',
      Item: { userId: '123', name: 'John Doe', email: 'john@example.com' },
      ConditionExpression: undefined,
      ReturnValues: 'NONE',
      ReturnConsumedCapacity: undefined,
      ReturnItemCollectionMetrics: undefined,
    });
  });

  it('should build PutCommand with condition expression', () => {
    const input = {
      tableName: 'Users',
      item: { userId: '123', name: 'John Doe' },
      conditionExpression: 'attribute_not_exists(#userId)',
      expressionAttributeNames: { '#userId': 'userId' },
    };

    const result = buildPutCommandInput(input);

    expect(result.ConditionExpression).toBe('attribute_not_exists(#userId)');
    expect(result.ExpressionAttributeNames).toEqual({ '#userId': 'userId' });
  });

  it('should use provided expression attribute names with condition expression', () => {
    const input = {
      tableName: 'Users',
      item: { userId: '123', name: 'John' },
      conditionExpression: 'attribute_not_exists(userId) OR #v = :v',
      expressionAttributeNames: { '#v': 'version' },
      expressionAttributeValues: { ':v': 1 },
    };

    const result = buildPutCommandInput(input);

    expect(result.ConditionExpression).toBe('attribute_not_exists(userId) OR #v = :v');
    expect(result.ExpressionAttributeNames).toEqual({ '#v': 'version' });
    expect(result.ExpressionAttributeValues).toEqual({ ':v': 1 });
  });

  it('should support different return values', () => {
    const input = {
      tableName: 'Users',
      item: { userId: '123', name: 'John' },
      returnValues: 'ALL_OLD' as const,
    };

    const result = buildPutCommandInput(input);

    expect(result.ReturnValues).toBe('ALL_OLD');
  });

  it('should include return consumed capacity and item collection metrics', () => {
    const input = {
      tableName: 'Users',
      item: { userId: '123' },
      returnConsumedCapacity: 'INDEXES' as const,
      returnItemCollectionMetrics: 'SIZE' as const,
    };

    const result = buildPutCommandInput(input);

    expect(result.ReturnConsumedCapacity).toBe('INDEXES');
    expect(result.ReturnItemCollectionMetrics).toBe('SIZE');
  });
});
