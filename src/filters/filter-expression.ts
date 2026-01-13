import { DynamoFilter, DynamoFiltersInput, DynamoFilterValueType, GenericRecord, StringRecord } from '../types';

export type GeneratedDynamoFilterAttributes = {
  filterExpression: string;
  expressionAttributeNames: StringRecord; // Record<string, string>;
  expressionAttributeValues: GenericRecord; // Record<string, unknown>;
};

/**
 * Generate DynamoDB filter attributes from the provided filters.
 *
 * ### Supported filter conditions:
 * - 'EQUAL_TO'
 * - 'NOT_EQUAL_TO'
 * - 'LESS_THAN'
 * - 'LESS_THAN_OR_EQUAL_TO'
 * - 'GREATER_THAN'
 * - 'GREATER_THAN_OR_EQUAL_TO'
 * - 'BETWEEN'
 * - 'BEGINS_WITH'
 * - 'CONTAINS'
 * - 'NOT_CONTAINS'
 * - 'EXISTS'
 * - 'NOT_EXISTS'
 *
 * ### Process flow:
 * 1. Validate input filters.
 * 2. Construct expression attribute names and values.
 * 3. Build the filter expression string.
 * 4. Return the generated attributes.
 *
 *
 * @param {DynamoFiltersInput} filters - The input filters to generate DynamoDB filter attributes from.
 * @returns {GeneratedDynamoFilterAttributes} - The generated DynamoDB filter attributes.
 */
export function generateDynamoFilterAttributes(filters: DynamoFiltersInput): GeneratedDynamoFilterAttributes | null {
  try {
    if (!filters || filters.length === 0) return null;

    const expressionAttributeNames: StringRecord = {};
    const expressionAttributeValues: GenericRecord = {};
    const parts: string[] = [];

    const makeNameToken = (i: number) => `#f${i}`;
    const makeValueToken = (i: number, suffix: string) => `:f${i}${suffix}`;

    for (let i = 0; i < filters.length; i++) {
      const filter = filters[i];
      const nameToken = makeNameToken(i);

      expressionAttributeNames[nameToken] = filter.name;

      switch (filter.condition) {
        case 'EXISTS': {
          parts.push(`(attribute_exists(${nameToken}))`);
          break;
        }

        case 'NOT_EXISTS': {
          parts.push(`(attribute_not_exists(${nameToken}))`);
          break;
        }

        case 'BEGINS_WITH': {
          if (filter.type !== 'S') {
            throw new Error(`BEGINS_WITH is only valid for type S (filter[${i}] has type ${filter.type})`);
          }
          const value = makeValueToken(i, 'v');
          expressionAttributeValues[value] = parseValueByType(filter.type, requireValue(filter, i));
          parts.push(`begins_with(${nameToken}, ${value})`);
          break;
        }

        case `CONTAINS`: {
          if (filter.type === 'SS' || filter.type === 'NS') {
            const vals = filter.values;
            if (!Array.isArray(vals) || vals.length === 0) {
              throw new Error(`CONTAINS with ${filter.type} requires values[] (non-empty) for filter[${i}]`);
            }

            const orParts: string[] = [];
            for (let j = 0; j < vals.length; j++) {
              const tok = makeValueToken(i, `vs${j}`);
              expressionAttributeValues[tok] = parseSetElement(filter.type, vals[j]);
              orParts.push(`contains(${nameToken}, ${tok})`);
            }

            parts.push(`(${orParts.length === 1 ? orParts[0] : `(${orParts.join(' OR ')})`})`);
          } else {
            const value = makeValueToken(i, 'v');
            expressionAttributeValues[value] = parseValueByType(filter.type, filter.value!);
            parts.push(`(contains(${nameToken}, ${value}))`);
          }
          break;
        }

        case `NOT_CONTAINS`: {
          if (filter.type === 'SS' || filter.type === 'NS') {
            const vals = filter.values;
            if (!Array.isArray(vals) || vals.length === 0) {
              throw new Error(`NOT_CONTAINS with ${filter.type} requires values[] (non-empty) for filter[${i}]`);
            }

            const orParts: string[] = [];
            for (let j = 0; j < vals.length; j++) {
              const tok = makeValueToken(i, `vs${j}`);
              expressionAttributeValues[tok] = parseSetElement(filter.type, vals[j]);
              orParts.push(`contains(${nameToken}, ${tok})`);
            }

            // NOT (A OR B OR C)
            parts.push(`(NOT (${orParts.join(' OR ')}))`);
          } else {
            const value = makeValueToken(i, 'v');
            expressionAttributeValues[value] = parseValueByType(filter.type, requireValue(filter, i));
            parts.push(`NOT contains(${nameToken}, ${value})`);
          }
          break;
        }

        case 'BETWEEN': {
          if (typeof filter.value !== 'string' || typeof filter.value2 !== 'string') {
            throw new Error(`BETWEEN requires value and value2 for filter[${i}]`);
          }
          if (filter.type === 'SS' || filter.type === 'NS' || filter.type === 'BOOL' || filter.type === 'NULL') {
            throw new Error(`BETWEEN is not valid for type ${filter.type} (filter[${i}])`);
          }
          const v1 = makeValueToken(i, 'v');
          const v2 = makeValueToken(i, 'v2');

          expressionAttributeValues[v1] = parseValueByType(filter.type, filter.value);
          expressionAttributeValues[v2] = parseValueByType(filter.type, filter.value2);
          parts.push(`(${nameToken} BETWEEN ${v1} AND ${v2})`);
          break;
        }

        case 'EQUAL_TO':
        case 'NOT_EQUAL_TO':
        case 'LESS_THAN':
        case 'LESS_THAN_OR_EQUAL_TO':
        case 'GREATER_THAN':
        case 'GREATER_THAN_OR_EQUAL_TO': {
          // DynamoDB comparators:
          if (filter.type === 'SS' || filter.type === 'NS') {
            throw new Error(
              `Comparator "${filter.condition}" is not supported for ${filter.type}; use CONTAINS/NOT_CONTAINS`
            );
          }

          const v = makeValueToken(i, 'v');
          expressionAttributeValues[v] = parseValueByType(filter.type, requireValue(filter, i));

          const op =
            filter.condition === 'EQUAL_TO'
              ? '='
              : filter.condition === 'NOT_EQUAL_TO'
                ? '<>'
                : filter.condition === 'LESS_THAN'
                  ? '<'
                  : filter.condition === 'LESS_THAN_OR_EQUAL_TO'
                    ? '<='
                    : filter.condition === 'GREATER_THAN'
                      ? '>'
                      : '>=';

          parts.push(`(${nameToken} ${op} ${v})`);
          break;
        }
        default: {
          // Exhaustive guard: if TS unions drift, you will catch it here at runtime.
          const _never: never = filter.condition as never;
          throw new Error(`Unsupported filter condition: ${String(_never)}`);
        }
      }
    }

    return {
      filterExpression: parts.join(' AND '),
      expressionAttributeNames,
      expressionAttributeValues,
    };
  } catch (error: unknown) {
    console.error('Error generating DynamoDB filter attributes:', error);
    throw error;
  }
}

/**
 * Parse a raw string value into the appropriate DynamoDB type.
 * @param type
 * @param raw
 * @returns
 */
export function parseValueByType(type: DynamoFilterValueType, raw: string): unknown {
  switch (type) {
    case 'S':
      return raw;
    case 'N': {
      const n = Number(raw);
      if (!Number.isFinite(n)) {
        throw new Error(`Invalid numeric value "${raw}" for type N`);
      }
      return n;
    }
    case 'BOOL': {
      // Be forgiving: accept "true"/"false" (any case) + "1"/"0"
      const normalized = raw.trim().toLowerCase();
      if (normalized === 'true' || normalized === '1') return true;
      if (normalized === 'false' || normalized === '0') return false;
      throw new Error(`Invalid boolean value "${raw}" for type BOOL`);
    }
    case 'NULL':
      return null;
    default:
      return raw;
  }
}

const parseSetElement = (type: DynamoFilterValueType, raw: string): unknown => {
  if (type === 'SS') return raw;
  if (type === 'NS') {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new Error(`Invalid numeric value "${raw}" for type NS element`);
    return n;
  }
  // Not a set type; fallback to scalar parsing.
  return parseValueByType(type, raw);
};

const requireValue = (f: DynamoFilter, i: number): string => {
  if (typeof f.value !== 'string') throw new Error(`${f.condition} requires value for filter[${i}]`);
  return f.value;
};
