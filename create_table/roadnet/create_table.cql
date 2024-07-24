CREATE TABLE IF NOT EXISTS roadnet (
            FromNodeId int,
            ToNodeId int,
            PRIMARY KEY (FromNodeId, ToNodeId)
        );