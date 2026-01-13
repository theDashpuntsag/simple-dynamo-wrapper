import { ScanCommandInput } from '@aws-sdk/lib-dynamodb';
import { CustomScanCommandInput, customScanCommandInputSch, GenericRecord, StringRecord } from '../types';
import { generateDynamoFilterAttributes } from '../filters';
import { QueryParseError } from '../error';

/**
 * Build the ScanCommandInput for DynamoDB Scan operation.
 *
 * ### Process flow
 * 1. Extracts relevant fields from the input object.
 * 2. Validates that only one of `filtersAttributes` or `filterExpression` is provided.
 * 3. If `filtersAttributes` is provided, generates the filter expression and attribute maps.
 * 4. If `filterExpression` is provided, merges it with any provided attribute names/values.
 * 5. Constructs the final `ScanCommandInput` object with all necessary fields.
 *
 * ### Notes
 * - If both `filtersAttributes` and `filterExpression` are provided, an error is thrown.
 * - Merges any extra attribute names/values provided in the input.
 * - Ensures that if placeholders are used in expressions, the corresponding attribute maps are present.
 *
 * ### Contract
 * - Input: {@link CustomScanCommandInput} containing scan parameters.
 * - Output: A fully-formed {@link ScanCommandInput} safe to pass to the AWS SDK.
 * - Error: Throws {@link QueryParseError} if input validation fails.
 *
 * ### Operational Notes and Best Practices
 * - Scans are expensive: they read every item in the table or index segment being scanned.
 * - Prefer Query over Scan for scalable patterns; Scan is usually for admin tools, backfills, periodic jobs, and limited datasets.
 * - Always consider:
 *  - ProjectionExpression to limit returned attributes.
 *  - Limit and pagination to control data volume.
 *  - Parallel scans for large datasets, but be cautious of throughput consumption.
 *
 *
 * @param custom
 * @returns
 */
export function buildScanCommandInput(custom: CustomScanCommandInput): ScanCommandInput {
  const {
    tableName,
    indexName,
    filtersAttributes,

    // Manual expression inputs
    filterExpression: manualFilterExpression,
    expressionAttributeNames: manualNames,
    expressionAttributeValues: manualValues,

    // Always-merge extras
    extraExpAttributeNames,
    extraExpAttributeValues,

    projectionExpression,
    limit,
    exclusiveStartKey,
    consistentRead,
    segment,
    totalSegments,
    returnConsumedCapacity,
    returnItemCollectionMetrics,
  } = customScanCommandInputSch.parse(custom);

  // Generate filter expressions from filter attributes
  if (filtersAttributes && filtersAttributes.length > 0) {
    throw new QueryParseError('Provide either filtersAttributes or filterExpression (not both).', 400);
  }

  // Start building the ScanCommandInput
  const input: ScanCommandInput = {
    TableName: tableName,
    ...(indexName ? { IndexName: indexName } : {}),
    ...(projectionExpression ? { ProjectionExpression: projectionExpression } : {}),
    ...(limit ? { Limit: limit } : {}),
    ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
    ...(consistentRead !== undefined ? { ConsistentRead: consistentRead } : {}),
    ...(segment !== undefined && totalSegments !== undefined ? { Segment: segment, TotalSegments: totalSegments } : {}),
    ...(returnConsumedCapacity ? { ReturnConsumedCapacity: returnConsumedCapacity } : {}),
    ...(returnItemCollectionMetrics ? { ReturnItemCollectionMetrics: returnItemCollectionMetrics } : {}),
  };

  // Auto-generated filterAttributes path
  if (filtersAttributes && filtersAttributes.length > 0) {
    const generated = generateDynamoFilterAttributes(filtersAttributes);
    if (!generated) throw new QueryParseError('Failed to generate filter expression from filtersAttributes.', 400);

    const { filterExpression, expressionAttributeNames, expressionAttributeValues } = generated;
    if (!filterExpression) throw new QueryParseError('Generated filterExpression is empty.', 400);

    input.FilterExpression = filterExpression;
    input.ExpressionAttributeNames = mergeAttrNames(expressionAttributeNames, extraExpAttributeNames);
    input.ExpressionAttributeValues = mergeAttrValues(expressionAttributeValues, extraExpAttributeValues);

    return input;
  }

  // Manual filterExpression path
  if (manualFilterExpression) {
    input.FilterExpression = manualFilterExpression;
    const needsNames = manualFilterExpression.includes('#');
    const needsValues = manualFilterExpression.includes(':');

    const mergedNames = mergeAttrNames(manualNames, extraExpAttributeNames);
    const mergedValues = mergeAttrValues(manualValues, extraExpAttributeValues);

    if (needsNames && !isNonEmptyObject(mergedNames)) {
      throw new QueryParseError(
        'filterExpression contains name placeholders (#) but ExpressionAttributeNames is missing.',
        400
      );
    }

    if (needsValues && !isNonEmptyObject(mergedValues)) {
      throw new QueryParseError(
        'filterExpression contains value placeholders (:) but ExpressionAttributeValues is missing.',
        400
      );
    }

    input.ExpressionAttributeNames = mergedNames;
    input.ExpressionAttributeValues = mergedValues;

    return input;
  }

  // No filter at all: still allow scan with projection/limit/pagination/etc.
  input.ExpressionAttributeNames = mergeAttrNames(manualNames, extraExpAttributeNames);
  input.ExpressionAttributeValues = mergeAttrValues(manualValues, extraExpAttributeValues);

  // If both merged maps are empty, remove to keep AWS input clean
  if (input.ExpressionAttributeNames && Object.keys(input.ExpressionAttributeNames).length === 0) {
    delete input.ExpressionAttributeNames;
  }

  if (input.ExpressionAttributeValues && Object.keys(input.ExpressionAttributeValues).length === 0) {
    delete input.ExpressionAttributeValues;
  }

  return input;
}

/**
 * Merge two Record<string, unknown> objects, with extra taking precedence.
 *
 * @param {Record<string, unknown>} base - The base record.
 * @param {Record<string, unknown>} extra - The extra record to merge, taking precedence.
 * @returns {Record<string, unknown> | undefined} - The merged record.
 */
function mergeAttrValues(base?: GenericRecord, extra?: GenericRecord): GenericRecord | undefined {
  if (!base && !extra) return undefined;
  return { ...(base ?? {}), ...(extra ?? {}) };
}

/**
 * Merge two Record<string, string> objects, with extra taking precedence.
 *
 * @param {Record<string, string>} base - The base attribute names.
 * @param {Record<string, string>} extra - The extra attribute names to merge, taking precedence.
 * @returns {Record<string, string> | undefined} - The merged attribute names.
 */
function mergeAttrNames(base?: StringRecord, extra?: StringRecord): StringRecord | undefined {
  if (!base && !extra) return undefined;
  return { ...(base ?? {}), ...(extra ?? {}) };
}

/**
 * Check if a value is a non-empty object.
 *
 * @param {unknown} v - The value to check.
 * @returns True if the value is a non-empty object, false otherwise.
 */
function isNonEmptyObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v as object).length > 0;
}
