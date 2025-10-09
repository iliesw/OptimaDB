import { DataTypes, WhereOpFor } from "./types";

const escapeIdent = (name: string) => `"${name.replace(/"/g, '""')}"`; // safe for reserved words
const escapeStr = (val: string) => `'${val.replace(/'/g, "''")}'`;

const BuildCondField = <T extends DataTypes>(
  Type: T,
  FieldName: string,
  op: WhereOpFor<T>,
  val: any
) => {
  const field = `\`${FieldName}\``;
  // Normalize NULL comparisons across all types
  if (val === null) {
    if (op === "$eq") return `${field} IS NULL`;
    if (op === "$ne") return `${field} IS NOT NULL`;
    throw new Error(`Unsupported operator ${op} for null value`);
  }
  switch (Type) {
    case DataTypes.Float:
    case DataTypes.Int: {
      const T: Record<string, string> = {
        $eq: "=",
        $ne: "!=",
        $gt: ">",
        $gte: ">=",
        $lt: "<",
        $lte: "<=",
        $in: "IN",
        $nin: "NOT IN",
      };
      if (["$in", "$nin"].includes(op)) {
        if (Array.isArray(val) && val.length) {
          return `${field} ${T[op]} (${val.join(", ")})`;
        }
        throw new Error(`${val} is not a valid array`);
      }
      return `${field} ${T[op]} ${val}`;
    }

    case DataTypes.Boolean: {
      const T: Record<string, string> = {
        $eq: "=",
        $ne: "!=",
      };
      return `${field} ${T[op]} ${val ? 1 : 0}`;
    }
    case DataTypes.Text: {
      const T: Record<string, string> = {
        $eq: "=",
        $ne: "!=",
        $like: "LIKE",
        $include: "LIKE", // include = SQL LIKE with wildcards
        $in: "IN",
        $nin: "NOT IN",
      };
      if (["$in", "$nin"].includes(op)) {
        if (Array.isArray(val) && val.length) {
          return `${field} ${T[op]} (${val.map(escapeStr).join(", ")})`;
        }
        throw new Error(`${val} is not a valid array`);
      }
      if (op === "$include") {
        return `${field} LIKE ${escapeStr(`%${val}%`)}`;
      }
      return `${field} ${T[op]} ${escapeStr(val)}`;
    }
    case DataTypes.UUID:
    case DataTypes.Email: {
      const T: Record<string, string> = {
        $eq: "=",
        $ne: "!=",
        $in: "IN",
        $nin: "NOT IN",
      };
      if (["$in", "$nin"].includes(op)) {
        if (Array.isArray(val) && val.length) {
          return `${field} ${T[op]} (${val.map(escapeStr).join(", ")})`;
        }
        throw new Error(`${val} is not a valid array`);
      }
      return `${field} ${T[op]} ${escapeStr(val)}`;
    }
    case DataTypes.Password: {
      const T: Record<string, string> = {
        $eq: "=",
        $ne: "!=",
      };
      return `${field} ${T[op]} ${escapeStr(val)}`;
    }
    case DataTypes.Date: {
      const normalizeDate = (v: Date | string) => {
        const DateOBJ = new Date(v);
        return DateOBJ.toISOString();
      };

      switch (op) {
        case "$eq":
          return `${field} = '${normalizeDate(val)}'`;
        case "$ne":
          return `${field} != '${normalizeDate(val)}'`;
        case "$before":
          return `${field} < '${normalizeDate(val)}'`;
        case "$ebefore":
          return `${field} <= '${normalizeDate(val)}'`;
        case "$after":
          return `${field} > '${normalizeDate(val)}'`;
        case "$eafter":
          return `${field} >= '${normalizeDate(val)}'`;
        case "$between":
          if (!Array.isArray(val) || val.length !== 2) {
            throw new Error(
              `$between operator requires an array of two values`
            );
          }
          return `(${field} >= '${normalizeDate(
            val[0]
          )}' AND ${field} <= '${normalizeDate(val[1])}')`;
        default:
          throw new Error(`Unsupported date operator: ${op}`);
      }
    }
    case DataTypes.Array: {
      switch (op) {
        case "$include": {
          if (Array.isArray(val)) {
            throw new Error(`${val} should be a single value, not an array`);
          }
          const formatted = typeof val === "string" ? `'${val}'` : val;
          return `array_contains(${field}, ${formatted}) = 1`;
        }

        case "$len": {
          if (typeof val !== "number") {
            throw new Error(`${val} must be a number`);
          }
          return `array_length(${field}) = ${val}`;
        }

        case "$eq": {
          if (Array.isArray(val)) {
            const formatted = val
              .map((v) => (typeof v === "string" ? `'${v}'` : v))
              .join(", ");
            return `${field} = array(${formatted})`;
          }
          throw new Error(`${val} must be an array for $eq`);
        }

        case "$ne": {
          if (Array.isArray(val)) {
            const formatted = val
              .map((v) => (typeof v === "string" ? `'${v}'` : v))
              .join(", ");
            return `${field} != array(${formatted})`;
          }
          throw new Error(`${val} must be an array for $ne`);
        }

        case "$at": {
          if (Array.isArray(val) && val.length === 2) {
            const [index, expected] = val;
            if (typeof index !== "number") {
              throw new Error(
                `First element of ${val} must be a number (index)`
              );
            }
            const formatted =
              typeof expected === "string" ? `'${expected}'` : expected;
            return `array_at(${field}, ${index}) = ${formatted}`;
          }
          throw new Error(`${val} must be [index, expectedValue] for $at`);
        }

        case "$index": {
          if (Array.isArray(val) && val.length === 2) {
            const [value, expectedIndex] = val;
            if (typeof expectedIndex !== "number") {
              throw new Error(
                `Second element of ${val} must be a number (index)`
              );
            }
            const formatted = typeof value === "string" ? `'${value}'` : value;
            return `array_index(${field}, ${formatted}) = ${expectedIndex}`;
          }
          throw new Error(`${val} must be [value, expectedIndex] for $index`);
        }

        default:
          throw new Error(`Unsupported array operator: ${op}`);
      }
    }

    case DataTypes.Json: {
      switch (op) {
        case "$eq": {
          return `${field} = ${escapeStr(JSON.stringify(val))}`;
        }
        case "$ne": {
          return `${field} != ${escapeStr(JSON.stringify(val))}`;
        }
        case "$in": {
          if (Array.isArray(val) && val.length) {
            return `${field} IN (${val
              .map((v) => escapeStr(JSON.stringify(v)))
              .join(", ")})`;
          }
          throw new Error(`${val} must be a non-empty array for $in`);
        }
        case "$nin": {
          if (Array.isArray(val) && val.length) {
            return `${field} NOT IN (${val
              .map((v) => escapeStr(JSON.stringify(v)))
              .join(", ")})`;
          }
          throw new Error(`${val} must be a non-empty array for $nin`);
        }
        case "$hasKey": {
          if (typeof val !== "string") {
            throw new Error(`${val} must be a string key for $hasKey`);
          }
          return `json_extract(${field}, '$.${val}') IS NOT NULL`;
        }
        case "$hasValue": {
          // checks if JSON contains a value anywhere (via json_each)
          return `EXISTS (SELECT 1 FROM json_each(${field}) WHERE value = ${escapeStr(
            val
          )})`;
        }
        case "$contains": {
          // check if json contains a sub-JSON (RFC 7396 style)
          return `json_type(${field}) = 'object' AND json_type(${escapeStr(
            JSON.stringify(val)
          )}) = 'object' AND json_patch(${field}, ${escapeStr(
            JSON.stringify(val)
          )}) = ${field}`;
        }
        default:
          throw new Error(`Unsupported JSON operator: ${op}`);
      }
    }

    default:
      throw new Error(`Unsupported DataType: ${Type}`);
  }
};
export const BuildCond = (CondObj, Schema) => {
  function condToGraph(condObj) {
    if (typeof condObj !== "object" || condObj === null) {
      // primitive value, not a valid condition
      return null;
    }
    // Handle logical operators at the top level
    if ("$or" in condObj) {
      return {
        type: "logic",
        op: "$or",
        children: condObj["$or"].map(condToGraph),
      };
    }
    if ("$and" in condObj) {
      return {
        type: "logic",
        op: "$and",
        children: condObj["$and"].map(condToGraph),
      };
    }
    return {
      type: "fields",
      children: Object.entries(condObj).map(([field, cond]) => ({
        type: "field",
        field,
        cond,
      })),
    };
  }

  function processNode(node: any) {
    if (node.type == "fields") {
      const Children = node.children;
      const ProcessedChildren = Children.map((c) => {
        return processNode(c);
      });
      return (ProcessedChildren as string[]).join(` AND `);
    } else if (node.type == "field") {
      const isEQ =
        Schema[node.field].Type != "JSON"
          ? typeof node.cond != "object" || node.cond === null
          : typeof node.cond == "object";
      if (isEQ) {
        return BuildCondField(
          Schema[node.field].Type,
          node.field,
          "$eq",
          node.cond
        );
      } else {
        const Keys = Object.keys(node.cond);
        if (Keys.length != 1) {
          throw new Error("Invalid Syntax");
        }
        return BuildCondField(
          Schema[node.field].Type,
          node.field,
          Keys[0],
          node.cond[Keys[0]]
        );
      }
    } else if (node.type == "logic") {
      const Children = node.children;
      const ProcessedChildren = Children.map((c) => {
        return processNode(c);
      });
      return (ProcessedChildren as string[])
        .map((e) => {
          return `( ${e} )`;
        })
        .join(` ${node.op == "$or" ? "OR" : "AND"} `);
    }
  }
  const Result = processNode(condToGraph(CondObj));
  return Result;
};
