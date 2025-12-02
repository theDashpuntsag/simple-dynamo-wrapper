import { describe, it, expect } from 'vitest';
import { buildUpdateCommandInput } from '../src/command';

describe('buildUpdateCommandInput', () => {
  it('should build update command with item (auto-generate update expression)', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John Doe', email: 'john@example.com' },
    };

    const result = buildUpdateCommandInput(input);

    expect(result.TableName).toBe('Users');
    expect(result.Key).toEqual({ userId: '123' });
    expect(result.UpdateExpression).toContain('SET');
    expect(result.UpdateExpression).toContain('name = ');
    expect(result.UpdateExpression).toContain('email = ');
    expect(result.ExpressionAttributeValues).toMatchObject({
      ':name': 'John Doe',
      ':email': 'john@example.com',
    });
  });

  it('should generate update expression from item', () => {
    const input = {
      tableName: 'Products',
      key: { productId: 'prod-1' },
      item: { active: true, metadata: { info: 'test' } },
    };

    const result = buildUpdateCommandInput(input);

    expect(result.UpdateExpression).toContain('SET');
    expect(result.UpdateExpression).toContain('active = ');
    expect(result.UpdateExpression).toContain('metadata = ');
    expect(result.ExpressionAttributeValues).toMatchObject({
      ':active': true,
      ':metadata': { info: 'test' },
    });
  });

  it('should use provided update expression directly', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      updateExpression: 'SET #name = :name, #count = #count + :inc',
      expressionAttributeNames: { '#name': 'name', '#count': 'count' },
      expressionAttributeValues: { ':name': 'Jane', ':inc': 1 },
    };

    const result = buildUpdateCommandInput(input);

    expect(result.UpdateExpression).toBe('SET #name = :name, #count = #count + :inc');
    expect(result.ExpressionAttributeNames).toEqual({ '#name': 'name', '#count': 'count' });
    expect(result.ExpressionAttributeValues).toEqual({ ':name': 'Jane', ':inc': 1 });
  });

  it('should merge extra expression attribute names and values', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
      extraExpAttributeNames: { '#custom': 'customField' },
      extraExpAttributeValues: { ':customValue': 'test' },
    };

    const result = buildUpdateCommandInput(input);

    expect(result.ExpressionAttributeNames).toMatchObject({ '#custom': 'customField' });
    expect(result.ExpressionAttributeValues).toMatchObject({ ':customValue': 'test' });
  });

  it('should include condition expression', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
      conditionExpression: 'attribute_exists(userId)',
    };

    const result = buildUpdateCommandInput(input);

    expect(result.ConditionExpression).toBe('attribute_exists(userId)');
  });

  it('should support return values', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
      returnValues: 'ALL_NEW' as const,
    };

    const result = buildUpdateCommandInput(input);

    expect(result.ReturnValues).toBe('ALL_NEW');
  });

  it('should default return values to NONE', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
    };

    const result = buildUpdateCommandInput(input);

    expect(result.ReturnValues).toBe('NONE');
  });

  it('should throw error when neither updateExpression nor item is provided', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
    };

    expect(() => buildUpdateCommandInput(input)).toThrow(
      'Either updateExpression or item with at least one field must be provided.'
    );
  });

  it('should throw error when item is empty object', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: {},
    };

    expect(() => buildUpdateCommandInput(input)).toThrow(
      'Either updateExpression or item with at least one field must be provided.'
    );
  });

  it('should avoid value key collisions', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
      expressionAttributeValues: { ':name': 'existing' },
    };

    const result = buildUpdateCommandInput(input);

    // Should create a different key for the item's name field
    expect(result.ExpressionAttributeValues).toMatchObject({ ':name': 'existing' });
    expect(result.UpdateExpression).toContain('name = :name_update_');
  });

  it('should include return consumed capacity and item collection metrics', () => {
    const input = {
      tableName: 'Users',
      key: { userId: '123' },
      item: { name: 'John' },
      returnConsumedCapacity: 'TOTAL' as const,
      returnItemCollectionMetrics: 'SIZE' as const,
    };

    const result = buildUpdateCommandInput(input);

    expect(result.ReturnConsumedCapacity).toBe('TOTAL');
    expect(result.ReturnItemCollectionMetrics).toBe('SIZE');
  });
});
