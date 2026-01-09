import { QueryParseError } from '../../error';
import { DynamoQueryRequest, dynamoQueryRequestSch } from '../../types';

export type QueryParamsType = { [name: string]: string | undefined };
export type EventParams = QueryParamsType | null;

/**
 * Extracts, normalizes, merges, and validates a DynamoDB query request derived from HTTP query parameters.
 *
 * This helper sits between API Gateway query strings (typically `event.queryStringParameters`) and your
 * internal DynamoDB query builder contract (`DynamoQueryRequest`).
 *
 * The function:
 * 1) Normalizes index selection (`indexName` preferred; otherwise `index`) into `indexName`
 * 2) Routes the request through one of several branches (no index, matched default index, custom index)
 * 3) Validates the resulting object with `dynamoQueryRequestSch`, throwing a 400 error on failure
 *
 * ---
 * ## Inputs
 *
 * @param qParams
 * Raw HTTP query parameters (stringly typed), typically from API Gateway.
 * Common fields include:
 * - `index` or `indexName`: target index name (normalized into `indexName`)
 * - `pKey`: partition key value
 * - `pKeyType`: partition key Dynamo type (`S | N | B`)
 * - `pKeyProp`: partition key attribute name in DynamoDB (e.g. "userId")
 * - `limit`: page size (string, coerced to positive int by Zod)
 * - optional sort key constraints: `sKey`, `sKeyType`, `sKeyProp`, `skComparator`, `skValue2`
 * - optional pagination: `lastEvaluatedKey` (object or stringified JSON, parsed by schema)
 * - optional sorting: `sorting` (`ASC | DESC`)
 *
 * @param defaultQry
 * Server-side default query template for the endpoint, used as a fallback and/or to fill missing fields.
 * This is expected to satisfy the schema requirements (at minimum: `pKey`, `pKeyType`, `pKeyProp`).
 *
 * ---
 * ## Normalization
 *
 * The function produces a single canonical index selector:
 * - If `qParams.indexName` is provided, it is used.
 * - Else if `qParams.index` is provided, it is copied into `indexName`.
 *
 * This ensures downstream code can rely on `indexName` for DynamoDB `IndexName`.
 *
 * ---
 * ## Routing workflow (matches current implementation)
 *
 * Let:
 * - `requestedIndex = queryParams.indexName` (after normalization)
 * - `isPKeyExists = pKey is a non-empty string`
 *
 * ### 1) No index provided (`!requestedIndex`)
 * - If `pKey` exists: validate `queryParams` as-is (strict)
 * - Else: merge defaults with query params and validate
 *
 * Additionally, in the merged-defaults path, the function forces `indexName` to `defaultQry.indexName`
 * (i.e., “no index provided” implies “use the endpoint’s default indexName”).
 *
 * ### 2) Index matches endpoint default (`requestedIndex === defaultQry.indexName`)
 * - If `pKey` exists: validate params as-is (strict) while forcing `indexName` to the default index name
 * - Else: fill missing fields from defaults (including `limit` fallback) and validate
 *
 * ### 3) Custom index (`requestedIndex !== defaultQry.indexName`)
 * - Validate the normalized params as-is (strict; no default merging)
 *
 * ---
 * ## Output
 *
 * @returns
 * A validated `DynamoQueryRequest` object (parsed by Zod).
 *
 * ---
 * ## Error handling
 *
 * All branches validate using `dynamoQueryRequestSch.safeParse`.
 * On validation failure, throws `CustomError(400)` with a flattened list of Zod issues.
 */
export function extractQueryReqFromParams(qParams: EventParams, defaultQry: DynamoQueryRequest): DynamoQueryRequest {
  const raw = qParams || {};
  const queryParams: QueryParamsType = {
    ...raw,
    indexName: raw.indexName ?? raw.index,
  };

  const requestedIndex = queryParams.indexName;
  const isPKeyExists = typeof queryParams.pKey === 'string' && queryParams.pKey.trim().length > 0;

  // No index provided
  if (!requestedIndex) {
    if (isPKeyExists) {
      return safeParseQuery(queryParams);
    }
    return safeParseQuery({ ...defaultQry, ...queryParams, indexName: defaultQry.indexName });
  }

  // Index matches the default index
  if (requestedIndex === defaultQry.indexName) {
    if (isPKeyExists) {
      return safeParseQuery({ ...queryParams, indexName: defaultQry.indexName });
    }

    return safeParseQuery({
      indexName: defaultQry.indexName,
      pKey: queryParams.pKey || defaultQry.pKey,
      pKeyType: queryParams.pKeyType || defaultQry.pKeyType,
      pKeyProp: queryParams.pKeyProp || defaultQry.pKeyProp,
      limit: Number(queryParams.limit || defaultQry.limit || `10`),
      sKey: queryParams.sKey || defaultQry.sKey,
      sKeyType: queryParams.sKeyType || defaultQry.sKeyType,
      sKeyProp: queryParams.sKeyProp || defaultQry.sKeyProp,
      skValue2: queryParams.skValue2 || defaultQry.skValue2,
      skComparator: queryParams.skComparator || defaultQry.skComparator,
      lastEvaluatedKey: queryParams.lastEvaluatedKey || defaultQry.lastEvaluatedKey,
      sorting: queryParams.sorting || defaultQry.sorting,
    });
  }

  // Custom index: validate as-is (indexName already normalized)
  return safeParseQuery(queryParams);
}

/**
 * Validates an unknown input against `dynamoQueryRequestSch` and returns the parsed output.
 *
 * Centralizing parsing here ensures consistent error handling for all branches:
 * - uniform logging format
 * - uniform `CustomError` thrown (400 Bad Request)
 *
 * @param query
 * Any value that is expected to represent a `DynamoQueryRequest` object. Commonly:
 * - `{...query}` (defaults)
 * - `{...query, ...queryParams}` (merged)
 * - `queryParams` directly
 *
 * @throws CustomError
 * Throws when Zod validation fails. The error message includes all Zod issues,
 * flattened into a comma-separated string containing `path: message` entries.
 *
 * @returns
 * The Zod-parsed `DynamoQueryRequest`.
 */
function safeParseQuery(query: unknown): DynamoQueryRequest {
  const parsedQuery = dynamoQueryRequestSch.safeParse(query);

  if (!parsedQuery.success) {
    const errorDetails = parsedQuery.error.issues.map((err) => `${err.path}: ${err.message}`).join(', ');
    throw new QueryParseError(`Bad request! ${errorDetails}`, 400);
  }

  return parsedQuery.data;
}
