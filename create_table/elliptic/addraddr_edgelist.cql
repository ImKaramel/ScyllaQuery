CREATE TABLE IF NOT EXISTS addraddr_edgelist (
    input_address text,
    output_address text,
    PRIMARY KEY (input_address, output_address)
);
