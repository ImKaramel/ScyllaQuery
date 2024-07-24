CREATE TABLE IF NOT EXISTS token_transfers (
        block_number int, 
        transaction_index int,  
        from_address text,
        to_address text, 
        time_stamp int, 
        contract_address text,
        value float,
        PRIMARY KEY (block_number, transaction_index, from_address, to_address, time_stamp, contract_address, value)
    );