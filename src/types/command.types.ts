import { z } from 'zod';
import { dynamoQueryRequestSch } from './dynamo-query-request.types';
import { dynamoFiltersInputSch } from './filter.types';

const returnConsumedCapacityOptionsSchema = z.enum(['INDEXES', 'TOTAL', 'NONE']);
const returnItemCollectionMetricsOptionsSchema = z.enum(['SIZE', 'NONE']);

const genericRecordSch = z.record(z.string(), z.unknown());
export type GenericRecord = z.infer<typeof genericRecordSch>;

const stringRecordSch = z.record(z.string(), z.string());
export type StringRecord = z.infer<typeof stringRecordSch>;

export const customGetCommandInputSchema = z.object({
  tableName: z.string(),
  key: genericRecordSch,
  projectionExpression: z.string().optional(),
  expressionAttributeNames: stringRecordSch.optional(),
  consistentRead: z.boolean().optional(),
  returnConsumedCapacity: returnConsumedCapacityOptionsSchema.optional(),
});

export type CustomGetCommandInput = z.infer<typeof customGetCommandInputSchema>;

export const customQueryCommandInputSchema = z.object({
  tableName: z.string(),
  queryRequest: dynamoQueryRequestSch,
  keyConditionExpression: z.string().optional(),
  filterExpression: z.string().optional(),
  expressionAttributeNames: stringRecordSch.optional(),
  expressionAttributeValues: genericRecordSch.optional(),
  extraExpAttributeNames: stringRecordSch.optional(),
  extraExpAttributeValues: genericRecordSch.optional(),
  projectionExpression: z.string().optional(),
  scanIndexForward: z.boolean().optional(),
  returnConsumedCapacity: returnConsumedCapacityOptionsSchema.optional(),
});

export type CustomQueryCommandInput = z.infer<typeof customQueryCommandInputSchema>;

export const customPutCommandInputSchema = z.object({
  tableName: z.string(),
  item: genericRecordSch,
  conditionExpression: z.string().optional(),
  expressionAttributeNames: stringRecordSch.optional(),
  expressionAttributeValues: genericRecordSch.optional(),
  returnValues: z.enum(['NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW']).optional(),
  returnConsumedCapacity: returnConsumedCapacityOptionsSchema.optional(),
  returnItemCollectionMetrics: returnItemCollectionMetricsOptionsSchema.optional(),
});

export type CustomPutCommandInput = z.infer<typeof customPutCommandInputSchema>;

export const customUpdateCommandInputSchema = z.object({
  tableName: z.string(),
  key: genericRecordSch,
  item: genericRecordSch.optional(),
  updateExpression: z.string().optional(),
  conditionExpression: z.string().optional(),
  expressionAttributeNames: stringRecordSch.optional(), // Overwrite ExpressionAttributeNames
  expressionAttributeValues: genericRecordSch.optional(), // Overwrite ExpressionAttributeValues
  extraExpAttributeNames: stringRecordSch.optional(), // Add ExpressionAttributeNames without overwriting existing ones
  extraExpAttributeValues: genericRecordSch.optional(), // Add ExpressionAttributeValues without overwriting existing ones
  returnValues: z.enum(['NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW']).optional(),
  returnConsumedCapacity: returnConsumedCapacityOptionsSchema.optional(),
  returnItemCollectionMetrics: returnItemCollectionMetricsOptionsSchema.optional(),
});

export type CustomUpdateCommandInput = z.infer<typeof customUpdateCommandInputSchema>;

export const customScanCommandInputSch = z
  .object({
    tableName: z.string(),
    indexName: z.string().optional(),
    filtersAttributes: dynamoFiltersInputSch.optional(),
    filterExpression: z.string().optional(),
    projectionExpression: z.string().optional(),
    expressionAttributeNames: stringRecordSch.optional(),
    expressionAttributeValues: genericRecordSch.optional(),
    extraExpAttributeNames: stringRecordSch.optional(), // Add ExpressionAttributeNames without overwriting existing ones
    extraExpAttributeValues: genericRecordSch.optional(), // Add ExpressionAttributeValues without overwriting existing ones
    limit: z.number().int().positive().optional(),
    exclusiveStartKey: genericRecordSch.optional(),
    consistentRead: z.boolean().optional(),
    segment: z.number().int().nonnegative().optional(),
    totalSegments: z.number().int().positive().optional(),
    returnConsumedCapacity: returnConsumedCapacityOptionsSchema.optional(),
    returnItemCollectionMetrics: z.enum(['SIZE', 'NONE']).optional(),
  })
  .superRefine((v, ctx) => {
    // Parallel scan validation
    const hasSeg = v.segment !== undefined;
    const hasTot = v.totalSegments !== undefined;

    if (hasSeg !== hasTot) {
      ctx.addIssue({
        code: 'custom',
        message: 'segment and totalSegments must be provided together for parallel scan.',
        path: hasSeg ? ['totalSegments'] : ['segment'],
      });
    }

    if (hasSeg && hasTot) {
      if (v.totalSegments! <= 0) {
        ctx.addIssue({
          code: 'custom',
          message: 'totalSegments must be > 0.',
          path: ['totalSegments'],
        });
      }
      if (v.segment! < 0 || v.segment! >= v.totalSegments!) {
        ctx.addIssue({
          code: 'custom',
          message: 'segment must be in range [0, totalSegments-1].',
          path: ['segment'],
        });
      }
    }

    // ConsistentRead is not supported on GSIs (DocumentClient will throw),
    // but you may want to enforce it here if indexName is present.
    if (v.indexName && v.consistentRead === true) {
      ctx.addIssue({
        code: 'custom',
        message: 'consistentRead cannot be true when scanning a GSI (indexName provided).',
        path: ['consistentRead'],
      });
    }
  });

export type CustomScanCommandInput = z.infer<typeof customScanCommandInputSch>;
