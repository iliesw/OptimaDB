export enum DataTypes {
  Int = "INT",
  Float = "FLOAT",
  Boolean = "BOOLEAN",
  Text = "TEXT",
  Password = "PASSWORD",
  Email = "EMAIL",
  Date = "DATE",
  UUID = "UUID",
  Array = "ARRAY",
  Json = "JSON",
  Vector = "Vector",
}

// Type-safe operator definitions
export const WhereOps = {
  [DataTypes.Int]: [
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
  ] as const,
  [DataTypes.Float]: [
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
  ] as const,
  [DataTypes.Boolean]: ["$eq", "$ne"] as const,
  [DataTypes.Text]: ["$eq", "$ne", "$in", "$nin", "$like", "$include"] as const,
  [DataTypes.Password]: ["$eq", "$ne"] as const,
  [DataTypes.Email]: ["$eq", "$ne", "$in", "$nin"] as const,
  [DataTypes.Date]: [
    "$eq",
    "$ne",
    "$before",
    "$ebefore",
    "$after",
    "$eafter",
    "$between",
  ] as const,
  [DataTypes.UUID]: ["$eq", "$ne", "$in", "$nin"] as const,
  [DataTypes.Array]: [
    "$include",
    "$len",
    "$eq",
    "$ne",
    "$at",
    "$index",
  ] as const, // array operators + equality
  [DataTypes.Json]: [
    "$eq",
    "$ne",
    "$in",
    "$nin",
    "$hasKey",
    "$hasValue",
    "$contains",
  ] as const,
  [DataTypes.Vector]: ["$dist"] as const,
} as const;

export type WhereOpFor<T extends DataTypes> = (typeof WhereOps)[T][number];
