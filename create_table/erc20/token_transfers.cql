CREATE TABLE IF NOT EXISTS token_transfers (
        block_number int, 
        transaction_index int,  
        from_address text,
        to_address text, 
        time_stamp int, 
        contract_address text,
        value float,
        actionid int PRIMARY KEY
    );
