import { Table, Int, Email,Array, Password, Json,Day, TableToSQL } from "../src";

export const Users = Table("Users", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  Email: Email(),
  Password: Password(),
});

export const Profile = Table("Profile", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  UserID: Int().reference(() => Users.ID),
  Likes: Int({
    default: 0,
  }),
  Bio: Json(),
  Win: Array()
});

export const Posts = Table("Posts", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  UserID: Int().reference(() => [Users.ID]),
  Content: Json(),
  CreatedAt: Day(),
});

export const Comments = Table("Comments", {
  ID: Int({
    default: 1,
    notNull: true,
  }),
  PostID: Int().reference(() => [Posts.ID]),
  UserID: Int().reference(() => [Users.ID]),
  Content: Json(),
  CreatedAt: Day(),
});

