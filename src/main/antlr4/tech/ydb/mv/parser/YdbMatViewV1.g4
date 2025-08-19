// SQL subset to define the materialized views for YDB.
grammar YdbMatViewV1;

// SQL Script is the sequence of SQL statements.
sql_script: SEMICOLON* sql_stmt (SEMICOLON+ sql_stmt)* SEMICOLON* EOF;

sql_stmt: create_mat_view_stmt | create_handler_stmt;

create_mat_view_stmt: CREATE ASYNC MATERIALIZED VIEW identifier AS simple_select_stmt;

create_handler_stmt: CREATE ASYNC HANDLER identifier
  handler_part (COMMA handler_part)* COMMA?;

handler_part: (handler_input_part | handler_process_part);

handler_process_part: PROCESS mat_view_ref;

handler_input_part: INPUT main_table_ref
  CHANGEFEED changefeed_name
  (CONSUMER consumer_name)?
  AS (STREAM | BATCH);

simple_select_stmt: SELECT result_column (COMMA result_column)* COMMA?
  FROM main_table_ref AS table_alias
  (simple_join_part)*
  (WHERE opaque_expression)?;

simple_join_part: (INNER | LEFT OUTER?)? JOIN join_table_ref AS table_alias
  ON join_condition (AND join_condition)*;

result_column: (column_reference | opaque_expression) AS column_alias;

opaque_expression: COMPUTE (ON table_alias (COMMA table_alias)*)? opaque_expression_body;
opaque_expression_body: OPAQUE_EXPRESSION;

fragment OPAQUE_BEGIN: '#[';
fragment OPAQUE_END: ']#';
OPAQUE_EXPRESSION: OPAQUE_BEGIN .*? OPAQUE_END;

join_condition: (column_reference_first | constant_first) EQUALS (column_reference_second | constant_second);
column_reference_first: column_reference;
column_reference_second: column_reference;
constant_first: integer_constant | string_constant;
constant_second: integer_constant | string_constant;

AND: A N D;
AS: A S;
ASYNC: A S Y N C;
BATCH: B A T C H;
CHANGEFEED: C H A N G E F E E D;
COMPUTE: C O M P U T E;
CONSUMER: C O N S U M E R;
CREATE: C R E A T E;
FROM: F R O M;
JOIN: J O I N;
HANDLER: H A N D L E R;
INNER: I N N E R;
INPUT: I N P U T;
LEFT: L E F T;
MATERIALIZED: M A T E R I A L I Z E D;
ON: O N;
OUTER: O U T E R;
PROCESS: P R O C E S S;
SELECT:  S E L E C T;
STREAM: S T R E A M;
VIEW: V I E W;
WHERE: W H E R E;

integer_constant: MINUS? DIGITS;
string_constant: (QUOTE_SINGLE (~('\'' | '\\') | ('\\' .))+? QUOTE_SINGLE);

column_reference: table_alias DOT column_name;

column_name: identifier;
main_table_ref: identifier;
mat_view_ref: identifier;
join_table_ref: identifier;
changefeed_name: identifier;
consumer_name: identifier;

table_alias: ID_PLAIN;
column_alias: ID_PLAIN;

identifier: ID_PLAIN | ID_QUOTED;

SEMICOLON: ';';
COMMA: ',';
DOT: '.';
MINUS: '-';
EQUALS: '=';
QUOTE_SINGLE: '\'';

fragment DIGIT: '0'..'9';
DIGITS: DIGIT+;

fragment BACKTICK: '`';
ID_PLAIN: ('a'..'z' | 'A'..'Z' | '_') ('a'..'z' | 'A'..'Z' | '_' | DIGIT)*;
ID_QUOTED: BACKTICK ('\\' . | '``' | ~('`' | '\\'))+? BACKTICK;

// case insensitive chars
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

fragment MULTILINE_COMMENT: '/*' .*? '*/';
fragment LINE_COMMENT: '--' ~('\n' | '\r')* ('\r' '\n'? | '\n' | EOF);
COMMENT: (MULTILINE_COMMENT | LINE_COMMENT) -> channel(HIDDEN);

WS: (' ' | '\r' | '\t' | '\n')+ -> channel(HIDDEN);
