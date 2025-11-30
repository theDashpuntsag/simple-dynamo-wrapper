import { DescribeTableCommand, DescribeTableCommandOutput } from '@aws-sdk/client-dynamodb';
import { docClient } from './dynamo-client';
import {
  CustomGetCommandInput,
  CustomPutCommandInput,
  CustomQueryCommandInput,
  CustomQueryCommandOutput,
  CustomUpdateCommandInput,
} from '../types/command.types';
import { GetCommandOutput, PutCommandOutput, QueryCommandOutput, UpdateCommandOutput } from '@aws-sdk/lib-dynamodb';

/**
 * Describe a DynamoDB table by name.
 *
 * This is a thin wrapper over {@link DescribeTableCommand}. It is useful for health checks,
 * migrations, or debugging table settings (e.g., key schema, throughput modes, GSIs).
 *
 * ### Contract
 * - Input: the DynamoDB table name.
 * - Output: the raw {@link DescribeTableCommandOutput} from AWS.
 * - Error: rethrows AWS service exceptions; wraps unknown errors.
 *
 * @param {string} tableName - The exact table name to describe.
 * @returns {Promise<DescribeTableCommandOutput>} The AWS response describing the table.
 * @throws When the table cannot be described (e.g., not found or IAM denied).
 *
 * @example
 * ```ts
 * const info = await getTableDescription(process.env.TABLE_NAME!);
 * console.log(info.Table?.KeySchema);
 * ```
 */
export async function getDynamoTableDescription(tableName: string): Promise<DescribeTableCommandOutput> {
  try {
    return await docClient.send(new DescribeTableCommand({ TableName: tableName }));
  } catch (error: unknown) {
    console.error(`Error getting table description for ${tableName}:`, error);
    throw error;
  }
}

/**
 * Get a single item by primary key.
 *
 * Provide a concrete type for `T` to get full type-safety for the returned item.
 * When no item exists for the provided key, `undefined` is returned.
 *
 * Contract
 * - Input: {@link CustomGetCommandInput} containing table name and key.
 * - Output: the item as `T`, or `undefined` if not found.
 * - Error: propagates AWS service exceptions; wraps unknown errors in {@link CustomError}.
 *
 * @typeParam T - The TypeScript interface/type representing the item shape stored in the table.
 * @param inputs - The key lookup parameters.
 * @returns The found item typed as `T`, or `undefined`.
 * @throws {@link CustomError} For unexpected error shapes.
 *
 * @example
 * ```ts
 * interface User {
 *   pk: string;
 *   sk: string;
 *   email: string;
 * }
 *
 * const user = await getRecordByKey<User>({
 *   tableName: process.env.TABLE_NAME!,
 *   key: { pk: 'USER#123', sk: 'PROFILE' }
 * });
 *
 * if (!user) {
 *   // handle not found
 * }
 * console.log(user.email);
 * ```
 */
export async function getDynamoRecordByKey(input: CustomGetCommandInput): Promise<GetCommandOutput> {
  try {
    const { GetCommand } = await import('@aws-sdk/lib-dynamodb');
    const { buildGetCommandInput } = await import('./build/build-get-command');
    const getCommand = buildGetCommandInput(input);
    return await docClient.send(new GetCommand(getCommand));
  } catch (error: unknown) {
    console.error(`Error getting record by key:`, error);
    throw error;
  }
}

/**
 * Query a DynamoDB table or secondary index and return typed items with pagination state.
 *
 * The returned `lastEvaluatedKey` can be supplied back into successive calls to fetch the
 * next page of results. Items are typed as `T` for end-to-end type-safety.
 *
 * Contract
 * - Input: {@link CustomQueryCommandInput} including table/index name, key condition, and optional pagination.
 * - Output: {@link CustomQueryCommandOutput} containing `items: T[]` and `lastEvaluatedKey`.
 * - Error: propagates AWS service exceptions; wraps unknown errors in {@link CustomError}.
 *
 * @typeParam T - The TypeScript interface/type representing the item shape stored in the table.
 * @param input - The query parameters (keys, expressions, limits, etc.).
 * @returns Paginated query output with typed items.
 *
 * @example Query first page
 * ```ts
 * const page1 = await queryRecords<User>({
 *   tableName: process.env.TABLE_NAME!,
 *   indexName: 'GSI1',
 *   keyCondition: {
 *     partitionKeyName: 'gsi1pk',
 *     partitionKeyValue: 'ORG#42',
 *   },
 *   limit: 25,
 * });
 * console.log(page1.items.length);
 * ```
 *
 * @example Paginate until completion
 * ```ts
 * let cursor: Record<string, unknown> | undefined;
 * do {
 *   const { items, lastEvaluatedKey } = await queryRecords<User>({
 *     tableName: process.env.TABLE_NAME!,
 *     keyCondition: { partitionKeyName: 'pk', partitionKeyValue: 'USER#123' },
 *     exclusiveStartKey: cursor,
 *     limit: 50,
 *   });
 *   // process items
 *   cursor = Object.keys(lastEvaluatedKey || {}).length ? lastEvaluatedKey : undefined;
 * } while (cursor);
 * ```
 */
export async function queryDynamoRecords(input: CustomQueryCommandInput): Promise<QueryCommandOutput> {
  try {
    const { QueryCommand } = await import('@aws-sdk/lib-dynamodb');
    const { buildQueryCommandInput } = await import('./build/build-query-command');
    const queryCommand = buildQueryCommandInput(input);
    const result = await docClient.send(new QueryCommand(queryCommand));
    return result;
  } catch (error: unknown) {
    console.error(`Error querying records:`, error);
    throw error;
  }
}

/**
 * Create (put) a record in DynamoDB and return the provided item.
 *
 * This helper delegates to {@link PutCommand}. By default, DynamoDB Put will blindly
 * overwrite an existing item with the same primary key. If you want conditional
 * writes (e.g., only create when not exists), ensure your {@link CustomPutCommandInput}
 * includes the appropriate condition expression via the command builder.
 *
 * Contract
 * - Input: {@link CustomPutCommandInput} carrying the item and table name.
 * - Output: returns the same `input.item` as `T` for convenience.
 * - Error: propagates AWS service exceptions; wraps unknown errors in {@link CustomError}.
 *
 * @typeParam T - The TypeScript interface/type representing the item shape stored in the table.
 * @param input - The put parameters including `item` and `tableName`.
 * @returns The original item `T`.
 *
 * @example
 * ```ts
 * const created = await createRecord<User>({
 *   tableName: process.env.TABLE_NAME!,
 *   item: { pk: 'USER#123', sk: 'PROFILE', email: 'x@y.z' },
 * });
 * ```
 */
export async function putDynamoRecord(input: CustomPutCommandInput): Promise<PutCommandOutput> {
  try {
    const { PutCommand } = await import('@aws-sdk/lib-dynamodb');
    const { buildPutCommandInput } = await import('./build/build-put-command');
    const putCommandInput = buildPutCommandInput(input);
    const result = await docClient.send(new PutCommand(putCommandInput));
    return result;
  } catch (error: unknown) {
    console.error(`Error putting record:`, error);
    throw error;
  }
}

/**
 * Update a record in DynamoDB and return the update result.
 *
 * This helper delegates to {@link UpdateCommand}. It builds the update expression
 * from the provided `item` fields unless an explicit `updateExpression` is given.
 *
 * Contract
 * - Input: {@link CustomUpdateCommandInput} carrying the key, item updates, and table name.
 * - Output: returns the {@link UpdateCommandOutput} from DynamoDB.
 * - Error: propagates AWS service exceptions; wraps unknown errors in {@link CustomError}.
 *
 * @typeParam T - The TypeScript interface/type representing the item shape stored in the table.
 * @param input - The update parameters including `key`, optional `item`, and `tableName`.
 * @returns The {@link UpdateCommandOutput} from DynamoDB.
 *
 * @example
 * ```ts
 * const result = await updateDynamoRecord<User>({
 *   tableName: process.env.TABLE_NAME!,
 *   key: { pk: 'USER#123', sk: 'PROFILE' },
 *   item: { email: 'newemail@example.com' },
 * });
 * ```
 */
export async function updateDynamoRecord(input: CustomUpdateCommandInput): Promise<UpdateCommandOutput> {
  try {
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    const { buildUpdateCommandInput } = await import('./build/build-update-command');
    const updateCommandInput = buildUpdateCommandInput(input);
    const result = await docClient.send(new UpdateCommand(updateCommandInput));
    return result;
  } catch (error: unknown) {
    console.error(`Error updating record:`, error);
    throw error;
  }
}
