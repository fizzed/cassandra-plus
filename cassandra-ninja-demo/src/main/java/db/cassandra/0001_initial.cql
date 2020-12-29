CREATE TABLE test (
  id uuid,
  name varchar,
  created_at timestamp,
  primary key ((id))
);

INSERT INTO test (id,name,created_at) VALUES (e4623c18-fcdd-4bde-9299-7722a4630c38,'Test','2018-03-05T13:00:01.123');