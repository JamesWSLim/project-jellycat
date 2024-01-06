SELECT distinct jellycat.jellycatid, size.price, size, stock FROM jellycat
LEFT JOIN size ON jellycat.jellycatname = size.jellycatname AND CONVERT = size.jellycatname