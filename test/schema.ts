import {
  Table,
  int,
  email,
  array,
  password,
  json,
  float,
  time,
  text,
  isFieldInsertOptional,
  uuid,
} from "../src";
export const Users = Table("Users", {
  ID: uuid(),
  Email: email({}),
  Password: password({ notNull: true }),
  isHuman: text({
    check(value) {
      return value.length > 10;
    },
  }),
  JSON: json({
    default: { X: 1, Y: 2 },
  }),
  Array: array({ default: [123, 1234] }),
  Dates: time({ default: new Date() }),
  Salary: float({ enum: [1.5, 1.7, 1.9] }),
});

export const Profile = Table("Profile", {
  ID: int({
    default: 1,
    notNull: true,
  }),
  UserID: int().reference(Users, "ID", "Many"),
  Likes: int({
    default: 0,
  }),
  Bio: json(),
  Win: array(),
});

export const Posts = Table("Posts", {
  ID: int({
    default: 1,
    notNull: true,
  }),
  UserID: int().reference(Users, "ID", "Many"),
  Content: json(),
});

export const Comments = Table("Comments", {
  ID: int({
    default: 1,
    notNull: true,
  }),
  PostID: int().reference(Posts, "ID", "Many"),
  UserID: int().reference(Users, "ID", "Many"),
  Content: json(),
});

