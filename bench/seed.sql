-- Canonical benchmark dataset, applied to the shared Postgres before a run so
-- BOTH servers (this Rust port and rocicorp/zero) sync the same data.
--
-- A single `issue` table with a chunk of rows + a publication. Idempotent.

DROP PUBLICATION IF EXISTS zero_bench_pub;
DROP TABLE IF EXISTS issue;

CREATE TABLE issue (
  id    text PRIMARY KEY,
  title text NOT NULL,
  owner text NOT NULL,
  open  boolean NOT NULL DEFAULT true,
  rank  int NOT NULL
);

-- 1,000 rows across 50 owners. This is large enough to exercise initial
-- replication without making benchmark startup dominate the ping workload.
INSERT INTO issue (id, title, owner, open, rank)
SELECT
  'i' || g,
  'issue number ' || g,
  'user' || (g % 50),
  (g % 3 <> 0),
  g
FROM generate_series(1, 1000) AS g;

CREATE PUBLICATION zero_bench_pub FOR TABLE issue;
