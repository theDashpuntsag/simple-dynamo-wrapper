import { describe, it, expect } from 'vitest';
import { generateDynamoFilterAttributes } from '../src/filters';
import { DynamoFilterCondition } from '../src/types';

describe('generateDynamoFilterAttributes', () => {
  describe('EXISTS condition', () => {
    it('should generate attribute_exists() expression', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EXISTS',
          type: 'S',
        },
      ]);

      expect(result?.filterExpression).toEqual('(attribute_exists(#f0))');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({});
    });

    it('should not include any expression attribute values', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EXISTS',
          type: 'S',
        },
      ]);

      expect(result?.expressionAttributeValues).toEqual({});
    });
  });

  describe('NOT_EXISTS condition', () => {
    it('should generate NOT attribute_exists() expression', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'NOT_EXISTS',
          type: 'S',
        },
      ]);

      expect(result?.filterExpression).toEqual('(attribute_not_exists(#f0))');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({});
    });

    it('should not include any expression attribute values', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'NOT_EXISTS',
          type: 'S',
        },
      ]);

      expect(result?.expressionAttributeValues).toEqual({});
    });
  });

  describe('BEGINS_WITH condition', () => {
    it('should generate begins_with() function with type S', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'BEGINS_WITH',
          type: 'S',
          value: 'prefix_',
        },
      ]);
      expect(result?.filterExpression).toEqual('begins_with(#f0, :f0v)');
    });

    it('should include proper name and value tokens', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'BEGINS_WITH',
          type: 'S',
          value: 'prefix_',
        },
      ]);

      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'prefix_' });
    });

    it('should throw error on non-string type (N, BOOL)', () => {
      expect(() =>
        generateDynamoFilterAttributes([
          {
            name: 'attribute1',
            condition: 'BEGINS_WITH',
            type: 'BOOL',
            value: 'true',
          },
        ])
      ).toThrow('BEGINS_WITH is only valid for type S');
    });
  });

  describe('EQUAL_TO condition', () => {
    it('should generate = operator with type S', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EQUAL_TO',
          type: 'S',
          value: 'testValue',
        },
      ]);

      expect(result?.filterExpression).toEqual('(#f0 = :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'testValue' });
    });

    it('should parse numeric value correctly with type N', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EQUAL_TO',
          type: 'N',
          value: '123.45',
        },
      ]);
      expect(result?.filterExpression).toEqual('(#f0 = :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 123.45 });
    });

    it('should parse boolean value correctly with type BOOL', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EQUAL_TO',
          type: 'BOOL',
          value: 'true',
        },
      ]);

      expect(result?.filterExpression).toEqual('(#f0 = :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': true });
    });

    it('should handle null value with type NULL', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'EQUAL_TO',
          type: 'NULL',
          value: 'null',
        },
      ]);

      expect(result?.filterExpression).toEqual('(#f0 = :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': null });
    });
  });

  describe('NOT_EQUAL_TO condition', () => {
    it('should generate <> operator', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'NOT_EQUAL_TO',
          type: 'S',
          value: 'testValue',
        },
      ]);

      expect(result?.filterExpression).toEqual('(#f0 <> :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'testValue' });
    });

    it('should work correctly with type N', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'attribute1',
          condition: 'NOT_EQUAL_TO',
          type: 'N',
          value: '42',
        },
      ]);

      expect(result?.filterExpression).toEqual('(#f0 <> :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'attribute1' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 42 });
    });
  });
  describe('Comparison operators', () => {
    it('should generate < operator for LESS_THAN', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'age',
          condition: 'LESS_THAN',
          type: 'N',
          value: '18',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 < :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'age' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 18 });
    });

    it('should generate <= operator for LESS_THAN_OR_EQUAL_TO', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'age',
          condition: 'LESS_THAN_OR_EQUAL_TO',
          type: 'N',
          value: '65',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 <= :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'age' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 65 });
    });

    it('should generate > operator for GREATER_THAN', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'score',
          condition: 'GREATER_THAN',
          type: 'N',
          value: '80',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 > :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'score' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 80 });
    });

    it('should generate >= operator for GREATER_THAN_OR_EQUAL_TO', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'score',
          condition: 'GREATER_THAN_OR_EQUAL_TO',
          type: 'N',
          value: '90',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 >= :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'score' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 90 });
    });

    it('should parse numbers correctly with comparison operators', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'price',
          condition: 'GREATER_THAN',
          type: 'N',
          value: '123.45',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 > :f0v)');
      expect(result?.expressionAttributeValues).toEqual({
        ':f0v': 123.45,
      });
      expect(typeof result?.expressionAttributeValues[':f0v']).toBe('number');
    });
  });

  describe('BETWEEN condition', () => {
    it('should generate BETWEEN ... AND ... expression', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'createdAt',
          condition: 'BETWEEN',
          type: 'N',
          value: '100',
          value2: '200',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 BETWEEN :f0v AND :f0v2)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'createdAt' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 100, ':f0v2': 200 });
    });

    it('should include both value and value2', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'amount',
          condition: 'BETWEEN',
          type: 'N',
          value: '1',
          value2: '9',
        },
      ]);

      expect(result?.expressionAttributeValues).toHaveProperty(':f0v', 1);
      expect(result?.expressionAttributeValues).toHaveProperty(':f0v2', 9);
    });

    it('should work with numeric ranges (type N)', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'score',
          condition: 'BETWEEN',
          type: 'N',
          value: '10.5',
          value2: '20.25',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 BETWEEN :f0v AND :f0v2)');
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 10.5, ':f0v2': 20.25 });
    });

    it('should work with string ranges (type S)', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'status',
          condition: 'BETWEEN',
          type: 'S',
          value: 'A',
          value2: 'Z',
        },
      ]);

      expect(result?.filterExpression).toBe('(#f0 BETWEEN :f0v AND :f0v2)');
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'A', ':f0v2': 'Z' });
    });

    it('should throw error on SS/NS/BOOL/NULL types', () => {
      const badTypes = ['SS', 'NS', 'BOOL', 'NULL'] as const;

      for (const t of badTypes) {
        expect(() =>
          generateDynamoFilterAttributes([
            {
              name: 'field',
              condition: 'BETWEEN',
              type: t,
              value: '1',
              value2: '2',
            },
          ])
        ).toThrow(/BETWEEN is not valid for type/);
      }
    });

    it('should throw error when value2 is missing', () => {
      expect(() =>
        generateDynamoFilterAttributes([
          {
            name: 'createdAt',
            condition: 'BETWEEN',
            type: 'N',
            value: '100',
            // value2 missing
          },
        ])
      ).toThrow(/BETWEEN requires value and value2/);
    });
  });

  describe('CONTAINS condition', () => {
    it('should generate contains() function with type S', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'title',
          condition: 'CONTAINS',
          type: 'S',
          value: 'abc',
        },
      ]);

      expect(result?.filterExpression).toBe('(contains(#f0, :f0v))');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'title' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'abc' });
    });

    it('should generate OR expression for multiple values with type SS', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'tags',
          condition: 'CONTAINS',
          type: 'SS',
          values: ['A', 'B', 'C'],
        },
      ]);

      expect(result?.filterExpression).toBe(
        '((contains(#f0, :f0vs0) OR contains(#f0, :f0vs1) OR contains(#f0, :f0vs2)))'
      );
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'tags' });
      expect(result?.expressionAttributeValues).toEqual({
        ':f0vs0': 'A',
        ':f0vs1': 'B',
        ':f0vs2': 'C',
      });
    });

    it('should generate OR expression with numeric values for type NS', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'nums',
          condition: 'CONTAINS',
          type: 'NS',
          values: ['1', '2', '3.5'],
        },
      ]);

      expect(result?.filterExpression).toBe(
        '((contains(#f0, :f0vs0) OR contains(#f0, :f0vs1) OR contains(#f0, :f0vs2)))'
      );
      expect(result?.expressionAttributeValues).toEqual({
        ':f0vs0': 1,
        ':f0vs1': 2,
        ':f0vs2': 3.5,
      });
    });

    it('should work correctly with type SS and single value', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'tags',
          condition: 'CONTAINS',
          type: 'SS',
          values: ['ONLY'],
        },
      ]);

      // implementation uses a single "contains(...)" without extra OR wrapper
      expect(result?.filterExpression).toBe('(contains(#f0, :f0vs0))');
      expect(result?.expressionAttributeValues).toEqual({ ':f0vs0': 'ONLY' });
    });

    it('should throw error with type SS and empty values array', () => {
      expect(() =>
        generateDynamoFilterAttributes([
          {
            name: 'tags',
            condition: 'CONTAINS',
            type: 'SS',
            values: [],
          },
        ])
      ).toThrow(/requires values\[\] \(non-empty\)/);
    });
  });

  describe('NOT_CONTAINS condition', () => {
    it('should generate NOT contains() expression with type S', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'title',
          condition: 'NOT_CONTAINS',
          type: 'S',
          value: 'abc',
        },
      ]);

      // NOTE: current implementation does NOT wrap with parentheses
      expect(result?.filterExpression).toBe('NOT contains(#f0, :f0v)');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'title' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'abc' });
    });

    it('should generate NOT (... OR ...) expression with type SS', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'tags',
          condition: 'NOT_CONTAINS',
          type: 'SS',
          values: ['A', 'B'],
        },
      ]);

      expect(result?.filterExpression).toBe('(NOT (contains(#f0, :f0vs0) OR contains(#f0, :f0vs1)))');
      expect(result?.expressionAttributeValues).toEqual({
        ':f0vs0': 'A',
        ':f0vs1': 'B',
      });
    });

    it('should handle numeric set values with type NS', () => {
      const result = generateDynamoFilterAttributes([
        {
          name: 'nums',
          condition: 'NOT_CONTAINS',
          type: 'NS',
          values: ['1', '2.25'],
        },
      ]);

      expect(result?.filterExpression).toBe('(NOT (contains(#f0, :f0vs0) OR contains(#f0, :f0vs1)))');
      expect(result?.expressionAttributeValues).toEqual({
        ':f0vs0': 1,
        ':f0vs1': 2.25,
      });
    });

    it('should throw error with type SS and empty values array', () => {
      expect(() =>
        generateDynamoFilterAttributes([
          {
            name: 'tags',
            condition: 'NOT_CONTAINS',
            type: 'SS',
            values: [],
          },
        ])
      ).toThrow(/requires values\[\] \(non-empty\)/);
    });
  });

  describe('Multiple filters', () => {
    it('should combine multiple filters with AND operator', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'age', condition: 'GREATER_THAN', type: 'N', value: '18' },
        { name: 'name', condition: 'CONTAINS', type: 'S', value: 'da' },
      ]);

      expect(result?.filterExpression).toBe('(#f0 > :f0v) AND (contains(#f1, :f1v))');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'age', '#f1': 'name' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 18, ':f1v': 'da' });
    });

    it('should generate unique name/value tokens (#f0, #f1, :f0v, :f1v)', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'age', condition: 'GREATER_THAN', type: 'N', value: '18' },
        { name: 'score', condition: 'LESS_THAN', type: 'N', value: '100' },
      ]);

      expect(result?.filterExpression).toBe('(#f0 > :f0v) AND (#f1 < :f1v)');
      expect(Object.keys(result?.expressionAttributeNames ?? {})).toEqual(['#f0', '#f1']);
      expect(Object.keys(result?.expressionAttributeValues ?? {})).toEqual([':f0v', ':f1v']);
    });

    it('should chain three or more filters correctly with AND', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'age', condition: 'GREATER_THAN', type: 'N', value: '18' },
        { name: 'country', condition: 'EQUAL_TO', type: 'S', value: 'MN' },
        { name: 'title', condition: 'CONTAINS', type: 'S', value: 'dev' },
      ]);

      expect(result?.filterExpression).toBe('(#f0 > :f0v) AND (#f1 = :f1v) AND (contains(#f2, :f2v))');
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'age', '#f1': 'country', '#f2': 'title' });
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 18, ':f1v': 'MN', ':f2v': 'dev' });
    });
  });

  describe('Type parsing - String (S)', () => {
    it('should preserve string value as-is', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'name', condition: 'EQUAL_TO', type: 'S', value: 'Dash' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 'Dash' });
    });
  });

  describe('Type parsing - Number (N)', () => {
    it('should parse valid numeric strings to numbers', () => {
      const result = generateDynamoFilterAttributes([{ name: 'age', condition: 'EQUAL_TO', type: 'N', value: '42' }]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 42 });
    });

    it('should throw error with invalid number string', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'age', condition: 'EQUAL_TO', type: 'N', value: 'not-a-number' }])
      ).toThrow(/Invalid numeric value/);
    });

    it('should parse decimal values correctly', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'price', condition: 'EQUAL_TO', type: 'N', value: '1.25' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': 1.25 });
    });

    it('should parse negative numbers correctly', () => {
      const result = generateDynamoFilterAttributes([{ name: 'delta', condition: 'EQUAL_TO', type: 'N', value: '-5' }]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': -5 });
    });
  });

  describe('Type parsing - Boolean (BOOL)', () => {
    it('should parse "true" to true', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'isActive', condition: 'EQUAL_TO', type: 'BOOL', value: 'true' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': true });
    });

    it('should parse "false" to false', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'isActive', condition: 'EQUAL_TO', type: 'BOOL', value: 'false' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': false });
    });

    it('should parse "1" to true', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'isActive', condition: 'EQUAL_TO', type: 'BOOL', value: '1' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': true });
    });

    it('should parse "0" to false', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'isActive', condition: 'EQUAL_TO', type: 'BOOL', value: '0' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': false });
    });

    it('should handle case-insensitive "TRUE"/"FALSE"', () => {
      const r1 = generateDynamoFilterAttributes([{ name: 'flag', condition: 'EQUAL_TO', type: 'BOOL', value: 'TRUE' }]);
      const r2 = generateDynamoFilterAttributes([
        { name: 'flag', condition: 'EQUAL_TO', type: 'BOOL', value: 'FaLsE' },
      ]);
      expect(r1?.expressionAttributeValues).toEqual({ ':f0v': true });
      expect(r2?.expressionAttributeValues).toEqual({ ':f0v': false });
    });

    it('should throw error with invalid boolean value', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'flag', condition: 'EQUAL_TO', type: 'BOOL', value: 'maybe' }])
      ).toThrow(/Invalid boolean value/);
    });
  });

  describe('Type parsing - NULL', () => {
    it('should return null value', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'deletedAt', condition: 'EQUAL_TO', type: 'NULL', value: 'anything' },
      ]);
      expect(result?.expressionAttributeValues).toEqual({ ':f0v': null });
    });
  });

  describe('Type parsing - String Set (SS)', () => {
    it('should handle array of strings with CONTAINS', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'tags', condition: 'CONTAINS', type: 'SS', values: ['x', 'y'] },
      ]);

      expect(result?.filterExpression).toBe('((contains(#f0, :f0vs0) OR contains(#f0, :f0vs1)))');
      expect(result?.expressionAttributeValues).toEqual({ ':f0vs0': 'x', ':f0vs1': 'y' });
    });

    it('should throw error with comparator operators (=, <, >)', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'tags', condition: 'EQUAL_TO', type: 'SS', value: 'x' }])
      ).toThrow(/Comparator "EQUAL_TO" is not supported for SS/);

      expect(() =>
        generateDynamoFilterAttributes([{ name: 'tags', condition: 'LESS_THAN', type: 'SS', value: 'x' }])
      ).toThrow(/Comparator "LESS_THAN" is not supported for SS/);

      expect(() =>
        generateDynamoFilterAttributes([{ name: 'tags', condition: 'GREATER_THAN', type: 'SS', value: 'x' }])
      ).toThrow(/Comparator "GREATER_THAN" is not supported for SS/);
    });
  });

  describe('Type parsing - Number Set (NS)', () => {
    it('should parse array of numeric strings with CONTAINS', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'nums', condition: 'CONTAINS', type: 'NS', values: ['1', '2.5'] },
      ]);

      expect(result?.expressionAttributeValues).toEqual({ ':f0vs0': 1, ':f0vs1': 2.5 });
    });

    it('should throw error with invalid number in array', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'nums', condition: 'CONTAINS', type: 'NS', values: ['1', 'bad'] }])
      ).toThrow(/Invalid numeric value "bad" for type NS element/);
    });

    it('should throw error with comparator operators', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'nums', condition: 'EQUAL_TO', type: 'NS', value: '1' }])
      ).toThrow(/Comparator "EQUAL_TO" is not supported for NS/);
    });
  });

  describe('Error conditions', () => {
    it('should return null for empty filters array', () => {
      expect(generateDynamoFilterAttributes([])).toBeNull();
    });

    it('should throw error when filter missing required value for comparators', () => {
      expect(() => generateDynamoFilterAttributes([{ name: 'age', condition: 'EQUAL_TO', type: 'N' }])).toThrow(
        /requires value/
      );
    });

    it('should throw error with unsupported condition', () => {
      expect(() =>
        generateDynamoFilterAttributes([
          { name: 'x', condition: 'SOME_UNKNOWN' as DynamoFilterCondition, type: 'S', value: 'v' },
        ])
      ).toThrow(/Unsupported filter condition/);
    });

    it('should throw error for BEGINS_WITH on non-string type', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'x', condition: 'BEGINS_WITH', type: 'N', value: '1' }])
      ).toThrow(/BEGINS_WITH is only valid for type S/);
    });

    it('should throw error for comparator on SS/NS types with helpful message', () => {
      expect(() =>
        generateDynamoFilterAttributes([{ name: 'x', condition: 'GREATER_THAN', type: 'SS', value: 'a' }])
      ).toThrow(/use CONTAINS\/NOT_CONTAINS/);

      expect(() =>
        generateDynamoFilterAttributes([{ name: 'x', condition: 'GREATER_THAN', type: 'NS', value: '1' }])
      ).toThrow(/use CONTAINS\/NOT_CONTAINS/);
    });
  });

  describe('Edge cases', () => {
    it('should work correctly with single filter', () => {
      const result = generateDynamoFilterAttributes([{ name: 'age', condition: 'EQUAL_TO', type: 'N', value: '1' }]);
      expect(result?.filterExpression).toBe('(#f0 = :f0v)');
    });

    it('should work with special characters in attribute name', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'profile.name', condition: 'EQUAL_TO', type: 'S', value: 'x' },
      ]);
      expect(result?.expressionAttributeNames).toEqual({ '#f0': 'profile.name' });
      expect(result?.filterExpression).toBe('(#f0 = :f0v)');
    });

    it('should generate correctly with large number of filters (10+)', () => {
      const filters = Array.from({ length: 11 }).map((_, i) => ({
        name: `f${i}`,
        condition: 'EQUAL_TO' as const,
        type: 'N' as const,
        value: String(i),
      }));

      const result = generateDynamoFilterAttributes(filters);

      // 11 conditions => 10 ANDs
      expect(result?.filterExpression.split(' AND ')).toHaveLength(11);

      // spot-check last token presence
      expect(result?.expressionAttributeNames).toHaveProperty('#f10', 'f10');
      expect(result?.expressionAttributeValues).toHaveProperty(':f10v', 10);
    });

    it('should wrap individual conditions in parentheses where the implementation does so', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'age', condition: 'GREATER_THAN', type: 'N', value: '1' },
        { name: 'title', condition: 'CONTAINS', type: 'S', value: 'x' },
        { name: 'createdAt', condition: 'BETWEEN', type: 'N', value: '1', value2: '2' },
        { name: 'attr', condition: 'EXISTS', type: 'S' },
        { name: 'prefix', condition: 'BEGINS_WITH', type: 'S', value: 'p' },
        { name: 'no', condition: 'NOT_CONTAINS', type: 'S', value: 'z' },
      ]);

      expect(result?.filterExpression).toBe(
        '(#f0 > :f0v) AND (contains(#f1, :f1v)) AND (#f2 BETWEEN :f2v AND :f2v2) AND (attribute_exists(#f3)) AND begins_with(#f4, :f4v) AND NOT contains(#f5, :f5v)'
      );
    });

    it('should generate unique tokens across multiple filters (no collisions)', () => {
      const result = generateDynamoFilterAttributes([
        { name: 'a', condition: 'EQUAL_TO', type: 'S', value: '1' },
        { name: 'b', condition: 'EQUAL_TO', type: 'S', value: '2' },
        { name: 'c', condition: 'EQUAL_TO', type: 'S', value: '3' },
      ]);

      expect(Object.keys(result?.expressionAttributeNames ?? {})).toEqual(['#f0', '#f1', '#f2']);
      expect(Object.keys(result?.expressionAttributeValues ?? {})).toEqual([':f0v', ':f1v', ':f2v']);
    });
  });
});
