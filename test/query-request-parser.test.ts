import { describe, it, expect } from 'vitest';
import { extractQueryReqFromParams } from '../src/utils/query-request';
import { QueryParseError } from '../src/error';
import { DynamoQueryRequest } from '../src/types';

describe('extractQueryReqFromParams', () => {
  const defaultQuery: DynamoQueryRequest = {
    indexName: 'USERS',
    pKey: 'DEFAULT_USER',
    pKeyType: 'S',
    pKeyProp: 'userId',
    limit: 20,
  };

  describe('Only Partition Key Related Params', () => {
    it('should parse when only pKey related params are passed (no index)', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('userId');
      expect(result.indexName).toBeUndefined();
      expect(result.sKey).toBeUndefined();
      expect(result.limit).toBeUndefined();
    });

    it('should parse when only pKey related params are passed with limit', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'customer456',
          pKeyType: 'S',
          pKeyProp: 'customerId',
          limit: '50',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('customer456');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('customerId');
      expect(result.limit).toBe(50);
    });

    it('should parse when only pKey related params with number type', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: '12345',
          pKeyType: 'N',
          pKeyProp: 'orderId',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('12345');
      expect(result.pKeyType).toBe('N');
      expect(result.pKeyProp).toBe('orderId');
    });

    it('should throw error when pKey is provided but pKeyType is missing', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: 'user123',
            pKeyProp: 'userId',
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });

    it('should throw error when pKey is provided but pKeyProp is missing', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: 'user123',
            pKeyType: 'S',
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });
  });

  describe('lastEvaluatedKey as empty string', () => {
    it('should parse successfully when lastEvaluatedKey is empty string', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          lastEvaluatedKey: '',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
      expect(result.lastEvaluatedKey).toBeUndefined();
    });

    it('should parse successfully when lastEvaluatedKey is only whitespace', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          lastEvaluatedKey: '   ',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
      expect(result.lastEvaluatedKey).toBeUndefined();
    });

    it('should parse lastEvaluatedKey when it is a valid JSON string', () => {
      const lastKey = { userId: { S: 'user123' }, timestamp: { N: '12345' } };
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          lastEvaluatedKey: JSON.stringify(lastKey),
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
      expect(result.lastEvaluatedKey).toEqual(lastKey);
    });

    it('should throw error when lastEvaluatedKey is invalid JSON string', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: 'user123',
            pKeyType: 'S',
            pKeyProp: 'userId',
            lastEvaluatedKey: 'invalid json{',
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });
  });

  describe('No index provided', () => {
    it('should use defaults when no params provided (null)', () => {
      const result = extractQueryReqFromParams(null, defaultQuery);

      // indexName from defaults is not preserved when no index in params
      expect(result.pKey).toBe('DEFAULT_USER');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('userId');
      expect(result.limit).toBe(20);
    });

    it('should use defaults when empty object provided', () => {
      const result = extractQueryReqFromParams({}, defaultQuery);

      // indexName from defaults is not preserved when no index in params
      expect(result.pKey).toBe('DEFAULT_USER');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('userId');
      expect(result.limit).toBe(20);
    });

    it('should merge query params with defaults when no pKey provided', () => {
      const result = extractQueryReqFromParams(
        {
          limit: '50',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('DEFAULT_USER');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('userId');
      expect(result.limit).toBe(50);
      // Note: indexName gets overridden to undefined due to spread behavior
      // This happens because queryParams has indexName: undefined which overwrites defaultQry.indexName
      expect(result.indexName).toBe('USERS');
    });

    it('should override defaults when pKey and related params provided', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'custom123',
          pKeyType: 'S',
          pKeyProp: 'customId',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('custom123');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('customId');
    });
  });

  describe('Index matches default index', () => {
    it('should use query params when pKey exists and index matches', () => {
      const result = extractQueryReqFromParams(
        {
          index: 'USERS',
          pKey: 'user999',
          pKeyType: 'S',
          pKeyProp: 'userId',
          limit: '100',
        },
        defaultQuery
      );

      expect(result.indexName).toBe('USERS');
      expect(result.pKey).toBe('user999');
      expect(result.limit).toBe(100);
    });

    it('should use indexName parameter when provided instead of index', () => {
      const result = extractQueryReqFromParams(
        {
          indexName: 'USERS',
          pKey: 'user888',
          pKeyType: 'S',
          pKeyProp: 'userId',
        },
        defaultQuery
      );

      expect(result.indexName).toBe('USERS');
      expect(result.pKey).toBe('user888');
    });

    it('should fill from defaults when index matches but no pKey provided', () => {
      const result = extractQueryReqFromParams(
        {
          index: 'USERS',
        },
        defaultQuery
      );

      expect(result.indexName).toBe('USERS');
      expect(result.pKey).toBe('DEFAULT_USER');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('userId');
      expect(result.limit).toBe(20);
    });

    it('should use custom limit when index matches and no pKey provided', () => {
      const result = extractQueryReqFromParams(
        {
          index: 'USERS',
          limit: '75',
        },
        defaultQuery
      );

      expect(result.indexName).toBe('USERS');
      expect(result.pKey).toBe('DEFAULT_USER');
      expect(result.limit).toBe(75);
    });

    it('should fallback to 10 when no limit in params or defaults (index match, no pKey)', () => {
      const defaultWithoutLimit: DynamoQueryRequest = {
        indexName: 'USERS',
        pKey: 'DEFAULT_USER',
        pKeyType: 'S',
        pKeyProp: 'userId',
      };

      const result = extractQueryReqFromParams(
        {
          index: 'USERS',
        },
        defaultWithoutLimit
      );

      expect(result.limit).toBe(10);
    });
  });

  describe('Custom index (not matching default)', () => {
    it('should parse custom index with all required params', () => {
      const result = extractQueryReqFromParams(
        {
          index: 'ORDERS',
          pKey: 'order123',
          pKeyType: 'S',
          pKeyProp: 'orderId',
        },
        defaultQuery
      );

      expect(result.indexName).toBe('ORDERS');
      expect(result.pKey).toBe('order123');
      expect(result.pKeyType).toBe('S');
      expect(result.pKeyProp).toBe('orderId');
    });

    it('should throw error when custom index lacks required params', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            index: 'ORDERS',
            pKey: 'order123',
            // Missing pKeyType and pKeyProp
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });
  });

  describe('Sort key parameters', () => {
    it('should parse with sort key params', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sKey: 'timestamp123',
          sKeyType: 'S',
          sKeyProp: 'timestamp',
          skComparator: '=',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
      expect(result.sKey).toBe('timestamp123');
      expect(result.sKeyType).toBe('S');
      expect(result.sKeyProp).toBe('timestamp');
      expect(result.skComparator).toBe('=');
    });

    it('should parse with sort key and BETWEEN comparator', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sKey: '100',
          sKeyType: 'N',
          sKeyProp: 'score',
          skComparator: 'BETWEEN',
          skValue2: '200',
        },
        defaultQuery
      );

      expect(result.sKey).toBe('100');
      expect(result.skComparator).toBe('BETWEEN');
      expect(result.skValue2).toBe('200');
    });

    it('should parse with sort key and BEGINS_WITH comparator', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sKey: 'prefix',
          sKeyType: 'S',
          sKeyProp: 'name',
          skComparator: 'BEGINS_WITH',
        },
        defaultQuery
      );

      expect(result.sKey).toBe('prefix');
      expect(result.skComparator).toBe('BEGINS_WITH');
    });

    it('should throw error when sort key comparator requires skValue2 but not provided', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: 'user123',
            pKeyType: 'S',
            pKeyProp: 'userId',
            sKey: '100',
            sKeyType: 'N',
            sKeyProp: 'score',
            skComparator: 'BETWEEN',
            // Missing skValue2
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });
  });

  describe('Sorting parameter', () => {
    it('should parse with sorting ASC', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sorting: 'ASC',
        },
        defaultQuery
      );

      expect(result.sorting).toBe('ASC');
    });

    it('should parse with sorting DESC', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sorting: 'DESC',
        },
        defaultQuery
      );

      expect(result.sorting).toBe('DESC');
    });

    it('should throw error with invalid sorting value', () => {
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: 'user123',
            pKeyType: 'S',
            pKeyProp: 'userId',
            sorting: 'INVALID',
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });
  });

  describe('Edge cases', () => {
    it('should trim whitespace from pKey', () => {
      const result = extractQueryReqFromParams(
        {
          pKey: '  user123  ',
          pKeyType: 'S',
          pKeyProp: 'userId',
        },
        defaultQuery
      );

      expect(result.pKey).toBe('user123');
    });

    it('should throw error when pKey is empty string (after trim fails validation)', () => {
      // Empty string pKey gets trimmed and fails min(1) validation
      expect(() =>
        extractQueryReqFromParams(
          {
            pKey: '',
            pKeyType: 'S',
            pKeyProp: 'userId',
            limit: '30',
          },
          defaultQuery
        )
      ).toThrow(QueryParseError);
    });

    it('should handle complex complete query params', () => {
      const lastKey = { userId: { S: 'user999' } };
      const result = extractQueryReqFromParams(
        {
          indexName: 'USERS',
          pKey: 'user123',
          pKeyType: 'S',
          pKeyProp: 'userId',
          sKey: '50',
          sKeyType: 'N',
          sKeyProp: 'score',
          skComparator: '>=',
          limit: '25',
          sorting: 'DESC',
          lastEvaluatedKey: JSON.stringify(lastKey),
        },
        defaultQuery
      );

      expect(result.indexName).toBe('USERS');
      expect(result.pKey).toBe('user123');
      expect(result.sKey).toBe('50');
      expect(result.skComparator).toBe('>=');
      expect(result.limit).toBe(25);
      expect(result.sorting).toBe('DESC');
      expect(result.lastEvaluatedKey).toEqual(lastKey);
    });

    it('should handle defaults with sort key parameters', () => {
      const defaultWithSortKey: DynamoQueryRequest = {
        indexName: 'USERS',
        pKey: 'DEFAULT_USER',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '0',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: '>',
      };

      const result = extractQueryReqFromParams(
        {
          index: 'USERS',
        },
        defaultWithSortKey
      );

      expect(result.sKey).toBe('0');
      expect(result.sKeyType).toBe('N');
      expect(result.sKeyProp).toBe('score');
      expect(result.skComparator).toBe('>');
    });
  });
});
