CREATE TABLE IF NOT EXISTS addrtx_edgelist (
     input_address text,
     txId int,
     PRIMARY KEY (input_address, txId) );
--у одного txId много адресов
