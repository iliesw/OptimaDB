import {
  Table,
  Int,
  Email,
  Array,
  Password,
  Json,
  Float,
  Time,
  Boolean,
} from "../src";

export const Users = Table("Users", {
  ID: Int({
    check: (e) => {
      return e > 0;
    },
    autoIncrement:true,
  }),
  Email: Email({ notNull: true }),
  Password: Password(),
  isHuman: Boolean(),
  JSON: Json({ default: { Field1: 123 } }),
  Array: Array({ default: [123, 1234] }),
  Dates: Time({ default: new Date() }),
  Salary: Float({ enum: [1.5, 1.7, 1.9] }),
});

export const Profile = Table("Profile", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  UserID: Int().reference(Users, "ID", "Many"),
  Likes: Int({
    default: 0,
  }),
  Bio: Json(),
  Win: Array(),
});

export const Posts = Table("Posts", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  UserID: Int().reference(Users, "ID", "Many"),
  Content: Json(),
});

export const Comments = Table("Comments", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  PostID: Int().reference(Posts, "ID", "Many"),
  UserID: Int().reference(Users, "ID", "Many"),
  Content: Json(),
});
