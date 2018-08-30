/*Table - E(i,j,v) where i,j are vertices and v is the weight*/


/* Finding Indegree of the vertices*/
SELECT 
j AS nodes, COUNT(j) AS indegree
FROM E
GROUP BY j;


/* Finding Outdegree of the vertices*/
SELECT
i AS nodes, COUNT(i) AS outdegree
FROM E
GROUP BY i;
