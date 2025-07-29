-- An example of YDB table for MV definitions
CREATE TABLE `mv/statements` (
   statement_no Int32 NOT NULL,
   statement_text Text NOT NULL,
   PRIMARY KEY(statement_no);
);
