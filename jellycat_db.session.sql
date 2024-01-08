SELECT t1.jellycatname,MIN(t1.category),MIN(t1.link),MIN(t1.imagelink) FROM jellycat t1
FULL OUTER JOIN jellycat t2 ON t1.jellycatname = t2.jellycatname 
GROUP BY t1.jellycatname
HAVING count(t1.jellycatname) > 1