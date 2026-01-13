import { describe, it, expect } from 'vitest';
import { buildScanCommandInput } from '../src/command';
import { QueryParseError } from '../src/error';

describe('buildScanCommandInput', () => {
  describe('Basic scan operations', () => {
    it('should build basic scan with table name only', () => {
      const input = {
        tableName: 'Users',
      };

      const result = buildScanCommandInput(input);

      expect(result.TableName).toBe('Users');
      expect(result.IndexName).toBeUndefined();
      expect(result.FilterExpression).toBeUndefined();
      expect(result.ExpressionAttributeNames).toBeUndefined();
      expect(result.ExpressionAttributeValues).toBeUndefined();
    });

    it('should build scan with index name', () => {
      const input = {
        tableName: 'Users',
        indexName: 'EmailIndex',
      };

      const result = buildScanCommandInput(input);

      expect(result.TableName).toBe('Users');
      expect(result.IndexName).toBe('EmailIndex');
    });

    it('should include projection expression', () => {
      const input = {
        tableName: 'Users',
        projectionExpression: 'userId, email, #name',
        extraExpAttributeNames: { '#name': 'name' },
      };

      const result = buildScanCommandInput(input);

      expect(result.ProjectionExpression).toBe('userId, email, #name');
      expect(result.ExpressionAttributeNames).toEqual({ '#name': 'name' });
    });

    it('should include limit', () => {
      const input = {
        tableName: 'Users',
        limit: 50,
      };

      const result = buildScanCommandInput(input);

      expect(result.Limit).toBe(50);
    });

    it('should include consistent read', () => {
      const input = {
        tableName: 'Users',
        consistentRead: true,
      };

      const result = buildScanCommandInput(input);

      expect(result.ConsistentRead).toBe(true);
    });
  });

  describe('Pagination', () => {
    it('should support pagination with exclusiveStartKey', () => {
      const input = {
        tableName: 'Users',
        exclusiveStartKey: { userId: 'user-123', sortKey: 'item-50' },
        limit: 25,
      };

      const result = buildScanCommandInput(input);

      expect(result.ExclusiveStartKey).toEqual({ userId: 'user-123', sortKey: 'item-50' });
      expect(result.Limit).toBe(25);
    });
  });

  describe('Parallel scan', () => {
    it('should configure parallel scan with segment and totalSegments', () => {
      const input = {
        tableName: 'LargeTable',
        segment: 0,
        totalSegments: 4,
      };

      const result = buildScanCommandInput(input);

      expect(result.Segment).toBe(0);
      expect(result.TotalSegments).toBe(4);
    });

    it('should handle different segment numbers', () => {
      const input = {
        tableName: 'LargeTable',
        segment: 2,
        totalSegments: 5,
      };

      const result = buildScanCommandInput(input);

      expect(result.Segment).toBe(2);
      expect(result.TotalSegments).toBe(5);
    });
  });

  describe('Manual filter expression', () => {
    it('should build scan with manual filter expression', () => {
      const input = {
        tableName: 'Users',
        filterExpression: '#age > :minAge',
        expressionAttributeNames: { '#age': 'age' },
        expressionAttributeValues: { ':minAge': 18 },
      };

      const result = buildScanCommandInput(input);

      expect(result.FilterExpression).toBe('#age > :minAge');
      expect(result.ExpressionAttributeNames).toEqual({ '#age': 'age' });
      expect(result.ExpressionAttributeValues).toEqual({ ':minAge': 18 });
    });

    it('should build scan with complex filter expression', () => {
      const input = {
        tableName: 'Products',
        filterExpression: '#price BETWEEN :minPrice AND :maxPrice AND #category = :cat',
        expressionAttributeNames: { '#price': 'price', '#category': 'category' },
        expressionAttributeValues: { ':minPrice': 10, ':maxPrice': 100, ':cat': 'Electronics' },
      };

      const result = buildScanCommandInput(input);

      expect(result.FilterExpression).toBe('#price BETWEEN :minPrice AND :maxPrice AND #category = :cat');
      expect(result.ExpressionAttributeNames).toEqual({ '#price': 'price', '#category': 'category' });
      expect(result.ExpressionAttributeValues).toEqual({
        ':minPrice': 10,
        ':maxPrice': 100,
        ':cat': 'Electronics',
      });
    });

    it('should merge extra attribute names with manual filter expression', () => {
      const input = {
        tableName: 'Users',
        filterExpression: '#status = :active',
        expressionAttributeNames: { '#status': 'status' },
        expressionAttributeValues: { ':active': 'ACTIVE' },
        extraExpAttributeNames: { '#extraName': 'extraAttribute' },
        extraExpAttributeValues: { ':extraValue': 'extra' },
      };

      const result = buildScanCommandInput(input);

      expect(result.ExpressionAttributeNames).toEqual({
        '#status': 'status',
        '#extraName': 'extraAttribute',
      });
      expect(result.ExpressionAttributeValues).toEqual({
        ':active': 'ACTIVE',
        ':extraValue': 'extra',
      });
    });

    it('should allow extra attributes to override manual attributes', () => {
      const input = {
        tableName: 'Users',
        filterExpression: '#status = :value',
        expressionAttributeNames: { '#status': 'oldStatus' },
        expressionAttributeValues: { ':value': 'old' },
        extraExpAttributeNames: { '#status': 'newStatus' },
        extraExpAttributeValues: { ':value': 'new' },
      };

      const result = buildScanCommandInput(input);

      expect(result.ExpressionAttributeNames).toEqual({ '#status': 'newStatus' });
      expect(result.ExpressionAttributeValues).toEqual({ ':value': 'new' });
    });
  });

  describe('Error handling for manual filter expressions', () => {
    it('should throw error when filter expression has name placeholders but no attribute names', () => {
      const input = {
        tableName: 'Users',
        filterExpression: '#age > :minAge',
        expressionAttributeValues: { ':minAge': 18 },
      };

      expect(() => buildScanCommandInput(input)).toThrow(QueryParseError);
      expect(() => buildScanCommandInput(input)).toThrow(
        'filterExpression contains name placeholders (#) but ExpressionAttributeNames is missing.'
      );
    });

    it('should throw error when filter expression has value placeholders but no attribute values', () => {
      const input = {
        tableName: 'Users',
        filterExpression: '#age > :minAge',
        expressionAttributeNames: { '#age': 'age' },
      };

      expect(() => buildScanCommandInput(input)).toThrow(QueryParseError);
      expect(() => buildScanCommandInput(input)).toThrow(
        'filterExpression contains value placeholders (:) but ExpressionAttributeValues is missing.'
      );
    });

    it('should allow filter expression without placeholders', () => {
      const input = {
        tableName: 'Users',
        filterExpression: 'attribute_exists(email)',
      };

      const result = buildScanCommandInput(input);

      expect(result.FilterExpression).toBe('attribute_exists(email)');
      expect(result.ExpressionAttributeNames).toBeUndefined();
      expect(result.ExpressionAttributeValues).toBeUndefined();
    });

    it('should allow filter expression with only name placeholders', () => {
      const input = {
        tableName: 'Users',
        filterExpression: 'attribute_exists(#email)',
        expressionAttributeNames: { '#email': 'email' },
      };

      const result = buildScanCommandInput(input);

      expect(result.FilterExpression).toBe('attribute_exists(#email)');
      expect(result.ExpressionAttributeNames).toEqual({ '#email': 'email' });
      expect(result.ExpressionAttributeValues).toBeUndefined();
    });

    it('should allow filter expression with only value placeholders', () => {
      const input = {
        tableName: 'Users',
        filterExpression: 'status = :active',
        expressionAttributeValues: { ':active': 'ACTIVE' },
      };

      const result = buildScanCommandInput(input);

      expect(result.FilterExpression).toBe('status = :active');
      expect(result.ExpressionAttributeNames).toBeUndefined();
      expect(result.ExpressionAttributeValues).toEqual({ ':active': 'ACTIVE' });
    });
  });

  describe('Return options', () => {
    it('should include returnConsumedCapacity', () => {
      const input = {
        tableName: 'Users',
        returnConsumedCapacity: 'TOTAL' as const,
      };

      const result = buildScanCommandInput(input);

      expect(result.ReturnConsumedCapacity).toBe('TOTAL');
    });
  });

  describe('Comprehensive scan configuration', () => {
    it('should build scan with all optional parameters', () => {
      const input = {
        tableName: 'Products',
        indexName: 'CategoryIndex',
        filterExpression: '#price > :minPrice AND #stock > :minStock',
        expressionAttributeNames: { '#price': 'price', '#stock': 'stock' },
        expressionAttributeValues: { ':minPrice': 10, ':minStock': 0 },
        projectionExpression: '#name, #price, #stock',
        extraExpAttributeNames: { '#name': 'productName' },
        limit: 100,
        exclusiveStartKey: { productId: 'prod-123' },
        consistentRead: false,
        returnConsumedCapacity: 'INDEXES' as const,
        returnItemCollectionMetrics: 'SIZE' as const,
      };

      const result = buildScanCommandInput(input);

      expect(result.TableName).toBe('Products');
      expect(result.IndexName).toBe('CategoryIndex');
      expect(result.FilterExpression).toBe('#price > :minPrice AND #stock > :minStock');
      expect(result.ExpressionAttributeNames).toEqual({
        '#price': 'price',
        '#stock': 'stock',
        '#name': 'productName',
      });
      expect(result.ExpressionAttributeValues).toEqual({ ':minPrice': 10, ':minStock': 0 });
      expect(result.ProjectionExpression).toBe('#name, #price, #stock');
      expect(result.Limit).toBe(100);
      expect(result.ExclusiveStartKey).toEqual({ productId: 'prod-123' });
      expect(result.ConsistentRead).toBe(false);
      expect(result.ReturnConsumedCapacity).toBe('INDEXES');
    });

    it('should build parallel scan with filter and projection', () => {
      const input = {
        tableName: 'BigDataTable',
        segment: 1,
        totalSegments: 8,
        filterExpression: '#active = :true',
        expressionAttributeNames: { '#active': 'isActive' },
        expressionAttributeValues: { ':true': true },
        projectionExpression: 'id, #active, updatedAt',
        limit: 500,
      };

      const result = buildScanCommandInput(input);

      expect(result.TableName).toBe('BigDataTable');
      expect(result.Segment).toBe(1);
      expect(result.TotalSegments).toBe(8);
      expect(result.FilterExpression).toBe('#active = :true');
      expect(result.Limit).toBe(500);
    });
  });

  describe('Empty attribute maps cleanup', () => {
    it('should remove empty ExpressionAttributeNames when no filter is provided', () => {
      const input = {
        tableName: 'Users',
        projectionExpression: 'userId, email',
      };

      const result = buildScanCommandInput(input);

      expect(result.ExpressionAttributeNames).toBeUndefined();
      expect(result.ExpressionAttributeValues).toBeUndefined();
    });

    it('should keep non-empty attribute maps', () => {
      const input = {
        tableName: 'Users',
        extraExpAttributeNames: { '#name': 'userName' },
      };

      const result = buildScanCommandInput(input);

      expect(result.ExpressionAttributeNames).toEqual({ '#name': 'userName' });
    });
  });
});
