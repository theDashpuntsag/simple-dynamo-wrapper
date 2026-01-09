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
 * 1) Normalizes index selection (`index` / `indexName` → `indexName`)
 * 2) Routes the request through one of several branches (default route vs matched index vs custom index)
 * 3) Validates the resulting object with `dynamoQueryRequestSch`, throwing a 400 error on failure
 *
 * ---
 * ## Inputs
 *
 * @param qParams
 * Raw HTTP query parameters (strongly-typed), typically from API Gateway.
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
 * Notes:
 * - All values are typically strings at the HTTP layer. Parsing/coercion is handled by `dynamoQueryRequestSch`.
 * - Unknown keys are ignored by Zod (unless the schema is made strict elsewhere).
 *
 * @param defaultQry
 * Server-side default query template for the endpoint, used as a fallback and/or to fill missing fields.
 * This is expected to satisfy the schema requirements (at minimum: `pKey`, `pKeyType`, `pKeyProp`).
 *
 * ---
 * ## Output
 *
 * @returns
 * A validated `DynamoQueryRequest` object (parsed by Zod). On success, the output is safe to pass into
 * your DynamoDB query builder (e.g., `buildQueryCommandInput`) because:
 * - `limit` is coerced to a positive integer when present
 * - `lastEvaluatedKey` is parsed into an object when provided as stringified JSON
 * - sort key constraints are validated for consistency (via `.superRefine`)
 *
 * ---
 * ## Normalization
 *
 * Before routing, the function normalizes the index selector:
 * - If the caller provided `indexName`, it is used.
 * - Else if the caller provided `index`, it is copied into `indexName`.
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
 *
 * **Behavior implemented (strict):**
 * - If `pKey` exists → validate **queryParams as-is**:
 *   - `return safeParseQuery(queryParams)`
 * - Else → merge defaults with query params and validate:
 *   - `return safeParseQuery({ ...defaultQry, ...queryParams })`
 *
 * Implications:
 * - If `pKey` is provided but `pKeyType`/`pKeyProp` are missing, validation fails (400),
 *   because the schema requires them.
 * - When no `pKey` is provided, defaults are used and may be overridden by any provided optional knobs.
 *
 * ### 2) Index matches endpoint default (`requestedIndex === defaultQry.indexName`)
 *
 * - If `pKey` exists → validate query params as-is (but force normalized indexName) and validate:
 *   - `return safeParseQuery({ ...queryParams, indexName: defaultQry.indexName })`
 * - Else → fill missing fields from defaults (including limit fallback) and validate:
 *   - builds a merged object where:
 *     - `indexName` is forced to `defaultQry.indexName`
 *     - `pKey`, `pKeyType`, `pKeyProp` fall back to defaults
 *     - `limit` falls back via `queryParams.limit -> defaultQry.limit -> "10"`
 *     - optional sort key / pagination / sorting fields fall back to defaults
 *
 * Implications:
 * - This branch supports “index-only” calls like `?index=USERS` by constructing a valid request from defaults.
 * - When `pKey` exists, the caller must supply the schema-required fields (or the parse will fail).
 *
 * ### 3) Custom index (`requestedIndex !== defaultQry.indexName`)
 *
 * - Validate the normalized query params as-is:
 *   - `return safeParseQuery(queryParams)`
 *
 * Implications:
 * - No default merging is performed for custom indexes.
 * - The caller must supply all schema-required fields (`pKey`, `pKeyType`, `pKeyProp`) and satisfy sort-key rules.
 *
 * ---
 * ## Error handling
 *
 * All branches validate using `dynamoQueryRequestSch.safeParse`.
 * On validation failure, a `QueryParseError` is thrown with status code 400 and a flattened list of Zod issues.
 *
 * ---
 * ## Examples
 *
 * ### Example 1: No params (defaults)
 * ```ts
 * extractQueryReqFromParams(null, {
 *   indexName: "USERS",
 *   pKey: "DEFAULT",
 *   pKeyType: "S",
 *   pKeyProp: "userId",
 *   limit: 20,
 * });
 * // => parsed defaults
 * ```
 *
 * ### Example 2: Default index, no pKey (fills from defaults)
 * ```ts
 * extractQueryReqFromParams(
 *   { index: "USERS" },
 *   { indexName: "USERS", pKey: "DEFAULT", pKeyType: "S", pKeyProp: "userId", limit: 50 }
 * );
 * // => { indexName:"USERS", pKey:"DEFAULT", pKeyType:"S", pKeyProp:"userId", limit:50, ... }
 * ```
 *
 * ### Example 3: Default index + pKey (strict: must include pKeyType/pKeyProp)
 * ```ts
 * extractQueryReqFromParams(
 *   { index: "USERS", pKey: "abc", pKeyType: "S", pKeyProp: "userId", limit: "25" },
 *   { indexName: "USERS", pKey: "DEFAULT", pKeyType: "S", pKeyProp: "userId" }
 * );
 * // => parsed request, limit coerced to 25
 * ```
 *
 * ### Example 4: Custom index (strict: validate as-is)
 * ```ts
 * extractQueryReqFromParams(
 *   { index: "ORDERS", indexName: "ORDERS", pKey: "order#1", pKeyType: "S", pKeyProp: "orderId" },
 *   { indexName: "USERS", pKey: "DEFAULT", pKeyType: "S", pKeyProp: "userId" }
 * );
 * // => parsed request targeting ORDERS index (if allowed by schema and Dynamo setup)
 * ```
 */
export function extractQueryReqFromParams(qParams: EventParams, defaultQry: DynamoQueryRequest): DynamoQueryRequest {
  const raw = qParams || {};
  const queryParams: QueryParamsType = {
    ...raw,
    indexName: raw.index ?? raw.indexName,
  };
  const requestedIndex = queryParams.indexName;
  const isPKeyExists = typeof queryParams.pKey === 'string' && queryParams.pKey.trim().length > 0;

  // `index` is missing
  if (!requestedIndex) {
    if (isPKeyExists) {
      return safeParseQuery(queryParams);
    }
    return safeParseQuery({ ...defaultQry, ...queryParams });
  }

  // Index matches the default index
  if (requestedIndex === defaultQry.indexName) {
    if (isPKeyExists) {
      // Strict: require client to provide pKeyType and pKeyProp too.
      // Friendly alternative: merge defaults when missing required fields.
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
