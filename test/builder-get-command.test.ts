import { describe, it, expect } from 'vitest';
import { buildGetCommandInput } from '../src/command';

describe('buildGetCommandInput', () => {
  it('should build basic GetCommand without projection', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
    };

    const result = buildGetCommandInput(input);

    expect(result).toEqual({
      TableName: 'Users',
      Key: { userId: '123' },
      ProjectionExpression: undefined,
      ConsistentRead: undefined,
      ReturnConsumedCapacity: undefined,
    });
  });

  it('should build GetCommand with projection expression', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      projectionExpression: 'name, email, #customAttr',
      expressionAttributeNames: { '#customAttr': 'customAttribute' },
    };

    const result = buildGetCommandInput(input);

    expect(result.TableName).toBe('Users');
    expect(result.Key).toEqual({ userId: '123' });
    expect(result.ProjectionExpression).toBe('#name, email, #customAttr');
    expect(result.ExpressionAttributeNames).toEqual({ '#customAttr': 'customAttribute', '#name': 'name' });
  });

  it('should handle projection expression', () => {
    const input = {
      tableName: 'Products',
      key: { productId: 'prod-1' },
      projectionExpression: 'name, size, #customField',
      expressionAttributeNames: { '#customField': 'myField' },
    };

    const result = buildGetCommandInput(input);

    expect(result.ProjectionExpression).toBe('#name, #size, #customField');
    expect(result.ExpressionAttributeNames).toEqual({
      '#customField': 'myField',
      '#name': 'name',
      '#size': 'size',
    });
  });

  it('should merge extra expression attribute names', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      projectionExpression: 'name, #email',
      expressionAttributeNames: { '#email': 'emailAddress', '#customAttr': 'customAttribute' },
    };

    const result = buildGetCommandInput(input);

    expect(result.ExpressionAttributeNames).toEqual({
      '#name': 'name',
      '#email': 'emailAddress',
      '#customAttr': 'customAttribute',
    });
  });

  it('should include consistent read and return consumed capacity', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      consistentRead: true,
      returnConsumedCapacity: 'TOTAL' as const,
    };

    const result = buildGetCommandInput(input);

    expect(result.ConsistentRead).toBe(true);
    expect(result.ReturnConsumedCapacity).toBe('TOTAL');
  });
});
