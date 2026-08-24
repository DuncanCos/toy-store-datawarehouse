-- Spark's JDBC writer creates tables but not schemas, so the two warehouse
-- schemas must exist before the first load.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA silver IS 'Tables de detail nettoyees et typees (analyse ad-hoc)';
COMMENT ON SCHEMA gold IS 'Agregats metier servant le dashboard';
