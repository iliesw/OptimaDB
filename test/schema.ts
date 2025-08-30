import {
  Table,
  Int,
  Email,
  Array,
  Password,
  Json,
  Day,
  Float,
  isFieldInsertOptional,
} from "../src";

export const Users = Table("Users", {
  ID: Int({
    primaryKey: true,
    autoIncrement: true,
  }),
  Email: Email({ notNull: true }),
  Password: Password(),
  JSON: Json(),
  Array:Array(),
  Salary: Float(),
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
  CreatedAt: Day(),
});

export const Comments = Table("Comments", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  PostID: Int().reference(Posts, "ID", "Many"),
  UserID: Int().reference(Users, "ID", "Many"),
  Content: Json(),
  CreatedAt: Day(),
});


