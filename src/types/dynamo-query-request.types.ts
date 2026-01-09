import { z } from 'zod';

export const dynamoAttrTypeSch = z.enum(['S', 'N', 'B', 'BOOL', 'NULL', 'SS', 'NS', 'BS', 'M', 'L']);
export type DynamoAttrType = z.infer<typeof dynamoAttrTypeSch>;

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
      return s; // keep string; superRefine will raise a clearer error
    }
  }

  return raw;
}, z.record(z.string(), z.unknown()).optional());

export type DynamoQueryRequest = {
  pKey: string;
  pKeyType: DynamoAttrType;
  pKeyProp: string;

  sKey?: string;
  sKeyType?: DynamoKeyAttrType;
  sKeyProp?: string;

  skValue2?: string;

  skComparator?: DynamoComparator;
  indexName?: string;

  limit?: number;
  lastEvaluatedKey?: Record<string, unknown>;

  sorting?: 'ASC' | 'DESC';
};

export const dynamoQueryRequestSch: z.ZodType<DynamoQueryRequest> = z
  .object({
    pKey: qpStringReq,
    pKeyType: dynamoKeyAttrTypeSch,
    pKeyProp: qpStringReq,
    sKey: qpStringOpt,
    sKeyType: dynamoKeyAttrTypeSch.optional(),
    sKeyProp: qpStringOpt,
    skValue2: qpStringOpt,
    skComparator: dynamoComparatorSch.optional(),
    indexName: qpStringOpt,
    limit: z.coerce.number().int().positive().optional(),
    lastEvaluatedKey: dynamoLastEvaluatedKeySch,
    sorting: z.enum(['ASC', 'DESC']).optional(),
  })
  .superRefine((data, ctx) => {
    if (typeof data.lastEvaluatedKey === 'string') {
      ctx.addIssue({
        path: ['lastEvaluatedKey'],
        code: 'custom',
        message: 'lastEvaluatedKey must be a JSON object or a stringified JSON object',
      });
    }

    if (data.skComparator) {
      if (!data.sKey) {
        ctx.addIssue({ path: ['sKey'], code: 'custom', message: 'sKey is required when skComparator is present' });
      }
      if (!data.sKeyProp) {
        ctx.addIssue({
          path: ['sKeyProp'],
          code: 'custom',
          message: 'sKeyProp is required when skComparator is present',
        });
      }
      if (!data.sKeyType) {
        ctx.addIssue({
          path: ['sKeyType'],
          code: 'custom',
          message: 'sKeyType is required when skComparator is present',
        });
      }

      if (data.skComparator === 'BETWEEN') {
        if (!data.skValue2) {
          ctx.addIssue({
            path: ['skValue2'],
            code: 'custom',
            message: 'skValue2 is required when skComparator is BETWEEN',
          });
        }
      } else {
        if (data.skValue2) {
          ctx.addIssue({
            path: ['skValue2'],
            code: 'custom',
            message: 'skValue2/skValue2Type are only allowed when skComparator is BETWEEN',
          });
        }
      }
    } else {
      const anySkStuff = data.sKey || data.sKeyProp || data.sKeyType || data.skValue2;
      if (anySkStuff) {
        ctx.addIssue({
          path: ['skComparator'],
          code: 'custom',
          message: 'skComparator is required when providing sort key conditions',
        });
      }
    }
  });
