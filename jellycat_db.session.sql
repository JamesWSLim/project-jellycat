SELECT t1.jellycatid,t1.jellycatname,t1.category,t1.link,t1.imagelink,t1.datecreated as jellycatdatecreated,
t2.size,t2.price,t2.stock,t2.height,t2.width
FROM jellycat t1
LEFT JOIN size t2 on t1.jellycatid=t2.jellycatid
WHERE date(t1.datecreated) = '2024-01-09'