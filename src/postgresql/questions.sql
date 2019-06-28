CREATE TABLE qtable (
 id serial primary key,
 qid integer,
 tags VARCHAR[],
 duration numeric(10,3),
 community varchar(200));
