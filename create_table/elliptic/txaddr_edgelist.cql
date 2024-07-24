CREATE TABLE IF NOT EXISTS txaddr_edgelist (
        txId int,
        output_address text,
        PRIMARY KEY (txId, output_address)
);
