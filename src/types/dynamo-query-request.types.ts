import { z } from 'zod';

export const dynamoAttrTypeSch = z.enum(['S', 'N', 'B', 'BOOL', 'NULL', 'SS', 'NS', 'BS', 'M', 'L']);
export type DynamoAttrType = z.infer<typeof dynamoAttrTypeSch>;

// Keys can only be S | N | B
export const dynamoKeyAttrTypeSch = z.enum(['S', 'N', 'B']);
export type DynamoKeyAttrType = z.infer<typeof dynamoKeyAttrTypeSch>;

export const dynamoComparatorSch = z.enum(['=', '<', '<=', '>', '>=', 'BETWEEN', 'BEGINS_WITH']);
export type DynamoComparator = z.infer<typeof dynamoComparatorSch>;

const qpStringReq = z.preprocess((v) => (typeof v === 'string' ? v.trim() : v), z.string().min(1));

const qpStringOpt = z.preprocess((v) => {
  if (v === '' || v === null || v === undefined) return undefined;
  return typeof v === 'string' ? v.trim() : v;
}, z.string().optional());

const dynamoLastEvaluatedKeySch = z.preprocess((raw) => {
  if (raw === '' || raw === null || raw === undefined) return undefined;
  if (typeof raw === 'object') return raw;

  if (typeof raw === 'string') {
    const s = raw.trim();
    if (!s) return undefined;
    try {
      return JSON.parse(s);
    } catch {
      // Let Zod fail; we’ll add a clearer error in superRefine.
      return s;
    }
  }

  return raw;
}, z.record(z.string(), z.unknown()).optional());

export const dynamoQueryRequestSch = z
  .object({
    pKey: qpStringReq,
    pKeyType: dynamoKeyAttrTypeSch,
    pKeyProp: qpStringReq,

    sKey: qpStringOpt,
    sKeyType: dynamoKeyAttrTypeSch.optional(),
    sKeyProp: qpStringOpt,

    skValue2: qpStringOpt,
    skValue2Type: dynamoKeyAttrTypeSch.optional(),

    skComparator: dynamoComparatorSch.optional(),
    indexName: qpStringOpt,
    limit: z.coerce.number().int().positive().optional(),
    lastEvaluatedKey: dynamoLastEvaluatedKeySch,
    sorting: z.enum(['ASC', 'DESC']).optional(),
  })
  .superRefine((data, ctx) => {
    // Better error when lastEvaluatedKey is an invalid JSON string
    // (will only happen when preprocess returned a string)
    if (typeof data.lastEvaluatedKey === 'string') {
      ctx.addIssue({
        path: ['lastEvaluatedKey'],
        code: z.ZodIssueCode.custom,
        message: 'lastEvaluatedKey must be a JSON object or a stringified JSON object',
      });
    }

    if (data.skComparator) {
      if (!data.sKey) {
        ctx.addIssue({
          path: ['sKey'],
          code: z.ZodIssueCode.custom,
          message: 'sKey is required when skComparator is present',
        });
      }
      if (!data.sKeyProp) {
        ctx.addIssue({
          path: ['sKeyProp'],
          code: z.ZodIssueCode.custom,
          message: 'sKeyProp is required when skComparator is present',
        });
      }
      if (!data.sKeyType) {
        ctx.addIssue({
          path: ['sKeyType'],
          code: z.ZodIssueCode.custom,
          message: 'sKeyType is required when skComparator is present',
        });
      }

      // BETWEEN requires a second value + type
      if (data.skComparator === 'BETWEEN') {
        if (!data.skValue2) {
          ctx.addIssue({
            path: ['skValue2'],
            code: z.ZodIssueCode.custom,
            message: 'skValue2 is required when skComparator is BETWEEN',
          });
        }
        if (!data.skValue2Type) {
          ctx.addIssue({
            path: ['skValue2Type'],
            code: z.ZodIssueCode.custom,
            message: 'skValue2Type is required when skComparator is BETWEEN',
          });
        }
      } else {
        // Optional strictness: disallow skValue2 for non-BETWEEN
        if (data.skValue2 || data.skValue2Type) {
          ctx.addIssue({
            path: ['skValue2'],
            code: z.ZodIssueCode.custom,
            message: 'skValue2/skValue2Type are only allowed when skComparator is BETWEEN',
          });
        }
      }
    } else {
      // Optional strictness: if no comparator, ensure SK-related fields are not half-provided
      const anySkStuff = data.sKey || data.sKeyProp || data.sKeyType || data.skValue2 || data.skValue2Type;
      if (anySkStuff) {
        ctx.addIssue({
          path: ['skComparator'],
          code: z.ZodIssueCode.custom,
          message: 'skComparator is required when providing sort key conditions',
        });
      }
    }
  });

export type DynamoQueryRequest = z.infer<typeof dynamoQueryRequestSch>;
