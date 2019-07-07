CREATE TABLE questions (
 id serial primary key,
 qid integer,
 tags VARCHAR(500) [],
 duration numeric(10,3),
 community varchar(200).
 pr_score numeric(10,2));
