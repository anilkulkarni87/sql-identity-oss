-- Build impacted subgraph nodes and edges for clustering updates.
-- impacted nodes = nodes touched by edges_new plus their 1-hop neighbors in edges_current

DROP TABLE IF EXISTS idr_work.impacted_nodes;
CREATE TABLE idr_work.impacted_nodes AS
SELECT DISTINCT left_entity_key AS entity_key FROM idr_work.edges_new
UNION
SELECT DISTINCT right_entity_key AS entity_key FROM idr_work.edges_new;

DROP TABLE IF EXISTS idr_work.subgraph_nodes;
CREATE TABLE idr_work.subgraph_nodes AS
SELECT DISTINCT entity_key FROM idr_work.impacted_nodes
UNION
SELECT DISTINCT e.left_entity_key AS entity_key
FROM idr_out.identity_edges_current e
JOIN idr_work.impacted_nodes n ON n.entity_key = e.right_entity_key
UNION
SELECT DISTINCT e.right_entity_key AS entity_key
FROM idr_out.identity_edges_current e
JOIN idr_work.impacted_nodes n ON n.entity_key = e.left_entity_key;

DROP TABLE IF EXISTS idr_work.subgraph_edges;
CREATE TABLE idr_work.subgraph_edges AS
SELECT e.left_entity_key, e.right_entity_key
FROM idr_out.identity_edges_current e
JOIN idr_work.subgraph_nodes a ON a.entity_key = e.left_entity_key
JOIN idr_work.subgraph_nodes b ON b.entity_key = e.right_entity_key;
