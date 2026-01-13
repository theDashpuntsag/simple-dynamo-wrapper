import { z } from 'zod';

export const dynamoFilterValueTypeSch = z.enum(['S', 'N', 'BOOL', 'NULL', 'SS', 'NS']);
export type DynamoFilterValueType = z.infer<typeof dynamoFilterValueTypeSch>;

export const dynamoFilterConditionSch = z.enum([
  'EQUAL_TO',
  'NOT_EQUAL_TO',
  'LESS_THAN',
  'LESS_THAN_OR_EQUAL_TO',
  'GREATER_THAN',
  'GREATER_THAN_OR_EQUAL_TO',
  'EXISTS',
  'NOT_EXISTS',
  'BEGINS_WITH',
  'CONTAINS',
  'NOT_CONTAINS',
  'BETWEEN',
]);
export type DynamoFilterCondition = z.infer<typeof dynamoFilterConditionSch>;

/**
 * DynamoFilter validation:
 * - BETWEEN => requires value + value2
 * - IN => requires values (non-empty)
 * - ATTRIBUTE_EXISTS / ATTRIBUTE_NOT_EXISTS => must NOT include value/value2/values
 * - Default (comparators, BEGINS_WITH, CONTAINS) => requires value, forbids value2/values
 * - SS/NS types require values (and only values)
 */
export const dynamoFilterSch = z
  .object({
    name: z.string().min(1),
    type: dynamoFilterValueTypeSch,
    condition: dynamoFilterConditionSch,
    value: z.string().optional(),
    value2: z.string().optional(),
    values: z.array(z.string().min(1)).min(1).optional(),
  })
  .superRefine((f, ctx) => {
    const hasValue = typeof f.value === 'string';
    const hasValue2 = typeof f.value2 === 'string';
    const hasValues = Array.isArray(f.values) && f.values.length > 0;

    // ATTRIBUTE_EXISTS / ATTRIBUTE_NOT_EXISTS: no payload fields allowed
    if (f.condition === 'EXISTS' || f.condition === 'NOT_EXISTS') {
      if (hasValue || hasValue2 || hasValues) {
        ctx.addIssue({
          code: 'custom',
          path: ['condition'],
          message: `${f.condition} must not include value/value2/values`,
        });
      }
      return;
    }

    // BETWEEN: requires value and value2, forbids values
    if (f.condition === 'BETWEEN') {
      if (!hasValue) {
        ctx.addIssue({ code: 'custom', path: ['value'], message: 'BETWEEN requires value' });
      }
      if (!hasValue2) {
        ctx.addIssue({ code: 'custom', path: ['value2'], message: 'BETWEEN requires value2' });
      }
      if (hasValues) {
        ctx.addIssue({ code: 'custom', path: ['values'], message: 'BETWEEN must not include values' });
      }
      return;
    }

    // Type SS/NS: should use values (and only values) for safety/consistency
    if (f.type === 'SS' || f.type === 'NS') {
      if (!hasValues) {
        ctx.addIssue({
          code: 'custom',
          path: ['values'],
          message: `${f.type} filter requires values (non-empty)`,
        });
      }
      if (hasValue || hasValue2) {
        ctx.addIssue({
          code: 'custom',
          path: ['type'],
          message: `${f.type} filter must not include value/value2; use values`,
        });
      }
      return;
    }

    // Default: requires single value; forbids value2/values
    if (!hasValue) {
      ctx.addIssue({ code: 'custom', path: ['value'], message: `${f.condition} requires value` });
    }
    if (hasValue2) {
      ctx.addIssue({
        code: 'custom',
        path: ['value2'],
        message: `${f.condition} must not include value2`,
      });
    }
    if (hasValues) {
      ctx.addIssue({
        code: 'custom',
        path: ['values'],
        message: `${f.condition} must not include values`,
      });
    }
  });

export type DynamoFilter = z.infer<typeof dynamoFilterSch>;

// Optional helper for a function input that accepts multiple filters:
export const dynamoFiltersInputSch = z.array(dynamoFilterSch).max(50);
export type DynamoFiltersInput = z.infer<typeof dynamoFiltersInputSch>;
