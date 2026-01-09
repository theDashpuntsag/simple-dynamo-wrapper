import { describe, it, expect } from 'vitest';
import { dynamoQueryRequestSch } from '../src/types';

describe('dynamoQueryRequest', () => {
  describe('Valid inputs - Partition Key only', () => {
    it('parses minimal valid input with only partition key (string type)', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKey).toBe('userId123');
        expect(result.data.pKeyType).toBe('S');
        expect(result.data.pKeyProp).toBe('userId');
      }
    });

    it('parses partition key with number type', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: '12345',
        pKeyType: 'N',
        pKeyProp: 'id',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKeyType).toBe('N');
      }
    });

    it('parses partition key with binary type', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'binarydata',
        pKeyType: 'B',
        pKeyProp: 'dataKey',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKeyType).toBe('B');
      }
    });

    it('trims whitespace from string fields', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: '  userId123  ',
        pKeyType: 'S',
        pKeyProp: '  userId  ',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKey).toBe('userId123');
        expect(result.data.pKeyProp).toBe('userId');
      }
    });
  });

  describe('Valid inputs - With Sort Key and Comparators', () => {
    it('parses with sort key using equals comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: 'timestamp123',
        sKeyType: 'S',
        sKeyProp: 'timestamp',
        skComparator: '=',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.sKey).toBe('timestamp123');
        expect(result.data.skComparator).toBe('=');
      }
    });

    it('parses with sort key using less than comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '100',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: '<',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('<');
        expect(result.data.sKeyType).toBe('N');
      }
    });

    it('parses with sort key using greater than comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '50',
        sKeyType: 'N',
        sKeyProp: 'age',
        skComparator: '>',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('>');
      }
    });

    it('parses with sort key using less than or equal comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '100',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: '<=',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('<=');
      }
    });

    it('parses with sort key using greater than or equal comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '50',
        sKeyType: 'N',
        sKeyProp: 'age',
        skComparator: '>=',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('>=');
      }
    });

    it('parses with BEGINS_WITH comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: 'prefix',
        sKeyType: 'S',
        sKeyProp: 'name',
        skComparator: 'BEGINS_WITH',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('BEGINS_WITH');
      }
    });

    it('parses with BETWEEN comparator and two values', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: 'BETWEEN',
        skValue2: '100',
        skValue2Type: 'N',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.skComparator).toBe('BETWEEN');
        expect(result.data.skValue2).toBe('100');
        expect(result.data.skValue2Type).toBe('N');
      }
    });
  });

  describe('Valid inputs - With optional fields', () => {
    it('parses with indexName', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        indexName: 'GSI1',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.indexName).toBe('GSI1');
      }
    });

    it('parses with limit as number', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        limit: 10,
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.limit).toBe(10);
      }
    });

    it('coerces limit from string to number', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        limit: '25',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.limit).toBe(25);
      }
    });

    it('parses with sorting ASC', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sorting: 'ASC',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.sorting).toBe('ASC');
      }
    });

    it('parses with sorting DESC', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sorting: 'DESC',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.sorting).toBe('DESC');
      }
    });

    it('parses with lastEvaluatedKey as object', () => {
      const lastKey = { userId: { S: 'user123' }, timestamp: { N: '12345' } };
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: lastKey,
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lastEvaluatedKey).toEqual(lastKey);
      }
    });

    it('parses lastEvaluatedKey from JSON string', () => {
      const lastKey = { userId: { S: 'user123' } };
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: JSON.stringify(lastKey),
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lastEvaluatedKey).toEqual(lastKey);
      }
    });

    it('handles empty string lastEvaluatedKey as undefined', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: '',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lastEvaluatedKey).toBeUndefined();
      }
    });
  });

  describe('Valid inputs - Complete examples', () => {
    it('parses complete query with all optional fields', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: 'BETWEEN',
        skValue2: '100',
        skValue2Type: 'N',
        indexName: 'ScoreIndex',
        limit: 50,
        sorting: 'DESC',
        lastEvaluatedKey: { userId: { S: 'user999' } },
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKey).toBe('userId123');
        expect(result.data.sKey).toBe('10');
        expect(result.data.indexName).toBe('ScoreIndex');
        expect(result.data.limit).toBe(50);
        expect(result.data.sorting).toBe('DESC');
      }
    });

    it('parses complete query with minimal optional fields', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.pKey).toBe('userId123');
      }
    });

    it('parses a lastEvaluatedKey when its just empty spaces or empty string', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: '',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lastEvaluatedKey).toBeUndefined();
      }
    });
  });

  describe('Invalid inputs - Missing required fields', () => {
    it('fails when pKey is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKeyType: 'S',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });

    it('fails when pKeyType is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });

    it('fails when pKeyProp is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
      });

      expect(result.success).toBe(false);
    });

    it('fails when pKey is empty string', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: '',
        pKeyType: 'S',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });

    it('fails when pKey is only whitespace', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: '   ',
        pKeyType: 'S',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });
  });

  describe('Invalid inputs - Sort key validation', () => {
    it('fails when skComparator is present but sKey is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        skComparator: '=',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const sKeyError = result.error.issues.find((issue) => issue.path.includes('sKey'));
        expect(sKeyError).toBeDefined();
        expect(sKeyError?.message).toContain('sKey is required when skComparator is present');
      }
    });

    it('fails when skComparator is present but sKeyType is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: 'value',
        sKeyProp: 'sortKey',
        skComparator: '=',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const sKeyTypeError = result.error.issues.find((issue) => issue.path.includes('sKeyType'));
        expect(sKeyTypeError).toBeDefined();
        expect(sKeyTypeError?.message).toContain('sKeyType is required when skComparator is present');
      }
    });

    it('fails when skComparator is present but sKeyProp is missing', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: 'value',
        sKeyType: 'S',
        skComparator: '=',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const sKeyPropError = result.error.issues.find((issue) => issue.path.includes('sKeyProp'));
        expect(sKeyPropError).toBeDefined();
        expect(sKeyPropError?.message).toContain('sKeyProp is required when skComparator is present');
      }
    });

    it('fails when BETWEEN comparator is used without skValue2', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: 'BETWEEN',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const skValue2Error = result.error.issues.find((issue) => issue.path.includes('skValue2'));
        expect(skValue2Error).toBeDefined();
        expect(skValue2Error?.message).toContain('skValue2 is required when skComparator is BETWEEN');
      }
    });

    it('fails when BETWEEN comparator is used without skValue2Type', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: 'BETWEEN',
        skValue2: '100',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const skValue2TypeError = result.error.issues.find((issue) => issue.path.includes('skValue2Type'));
        expect(skValue2TypeError).toBeDefined();
        expect(skValue2TypeError?.message).toContain('skValue2Type is required when skComparator is BETWEEN');
      }
    });

    it('fails when skValue2 is provided with non-BETWEEN comparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: '>',
        skValue2: '100',
        skValue2Type: 'N',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const skValue2Error = result.error.issues.find((issue) => issue.path.includes('skValue2'));
        expect(skValue2Error).toBeDefined();
        expect(skValue2Error?.message).toContain('skValue2/skValue2Type are only allowed when skComparator is BETWEEN');
      }
    });

    it('fails when sort key fields are provided without skComparator', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const comparatorError = result.error.issues.find((issue) => issue.path.includes('skComparator'));
        expect(comparatorError).toBeDefined();
        expect(comparatorError?.message).toContain('skComparator is required when providing sort key conditions');
      }
    });
  });

  describe('Invalid inputs - Invalid types', () => {
    it('fails when pKeyType is not a valid key type', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'BOOL', // BOOL is not a valid key type
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });

    it('fails when pKeyType is invalid string', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'STRING',
        pKeyProp: 'userId',
      });

      expect(result.success).toBe(false);
    });

    it('fails when skComparator is invalid', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sKey: '10',
        sKeyType: 'N',
        sKeyProp: 'score',
        skComparator: 'EQUALS', // Invalid comparator
      });

      expect(result.success).toBe(false);
    });

    it('fails when limit is negative', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        limit: -5,
      });

      expect(result.success).toBe(false);
    });

    it('fails when limit is zero', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        limit: 0,
      });

      expect(result.success).toBe(false);
    });

    it('fails when limit is not an integer', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        limit: 10.5,
      });

      expect(result.success).toBe(false);
    });

    it('fails when sorting is invalid', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        sorting: 'ASCENDING', // Invalid, should be ASC or DESC
      });

      expect(result.success).toBe(false);
    });

    it('fails when lastEvaluatedKey is invalid JSON string', () => {
      const result = dynamoQueryRequestSch.safeParse({
        pKey: 'userId123',
        pKeyType: 'S',
        pKeyProp: 'userId',
        lastEvaluatedKey: 'invalid json{',
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const lastKeyError = result.error.issues.find((issue) => issue.path.includes('lastEvaluatedKey'));
        expect(lastKeyError).toBeDefined();
        expect(lastKeyError?.message).toContain('Invalid input: expected record, received string');
      }
    });
  });
});
