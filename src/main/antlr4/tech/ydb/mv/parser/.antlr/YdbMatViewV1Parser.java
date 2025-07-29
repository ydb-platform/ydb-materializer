// Generated from /home/zinal/Projects/YDB/integrations/ydb-materializer/src/main/antlr4/tech/ydb/mv/parser/YdbMatViewV1.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class YdbMatViewV1Parser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, OPAQUE_EXPRESSION=2, AND=3, AS=4, ASYNC=5, BATCH=6, CHANGEFEED=7, 
		COMPUTE=8, CREATE=9, FROM=10, JOIN=11, INNER=12, LEFT=13, MATERIALIZED=14, 
		ON=15, OUTER=16, PROCESS=17, SELECT=18, STREAM=19, VIEW=20, WHERE=21, 
		SEMICOLON=22, COMMA=23, DOT=24, MINUS=25, EQUALS=26, QUOTE_SINGLE=27, 
		DIGITS=28, ID_PLAIN=29, ID_QUOTED=30, COMMENT=31, WS=32;
	public static final int
		RULE_sql_script = 0, RULE_sql_stmt = 1, RULE_create_mat_view_stmt = 2, 
		RULE_process_stmt = 3, RULE_simple_select_stmt = 4, RULE_simple_join_part = 5, 
		RULE_result_column = 6, RULE_opaque_expression = 7, RULE_opaque_expression_body = 8, 
		RULE_join_condition = 9, RULE_column_reference_first = 10, RULE_column_reference_second = 11, 
		RULE_constant_first = 12, RULE_constant_second = 13, RULE_integer_constant = 14, 
		RULE_string_constant = 15, RULE_column_reference = 16, RULE_column_name = 17, 
		RULE_main_table_ref = 18, RULE_join_table_ref = 19, RULE_changefeed_name = 20, 
		RULE_table_alias = 21, RULE_column_alias = 22, RULE_identifier = 23;
	private static String[] makeRuleNames() {
		return new String[] {
			"sql_script", "sql_stmt", "create_mat_view_stmt", "process_stmt", "simple_select_stmt", 
			"simple_join_part", "result_column", "opaque_expression", "opaque_expression_body", 
			"join_condition", "column_reference_first", "column_reference_second", 
			"constant_first", "constant_second", "integer_constant", "string_constant", 
			"column_reference", "column_name", "main_table_ref", "join_table_ref", 
			"changefeed_name", "table_alias", "column_alias", "identifier"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'\\'", null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, "';'", "','", 
			"'.'", "'-'", "'='", "'''"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, "OPAQUE_EXPRESSION", "AND", "AS", "ASYNC", "BATCH", "CHANGEFEED", 
			"COMPUTE", "CREATE", "FROM", "JOIN", "INNER", "LEFT", "MATERIALIZED", 
			"ON", "OUTER", "PROCESS", "SELECT", "STREAM", "VIEW", "WHERE", "SEMICOLON", 
			"COMMA", "DOT", "MINUS", "EQUALS", "QUOTE_SINGLE", "DIGITS", "ID_PLAIN", 
			"ID_QUOTED", "COMMENT", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "YdbMatViewV1.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public YdbMatViewV1Parser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class Sql_scriptContext extends ParserRuleContext {
		public List<Sql_stmtContext> sql_stmt() {
			return getRuleContexts(Sql_stmtContext.class);
		}
		public Sql_stmtContext sql_stmt(int i) {
			return getRuleContext(Sql_stmtContext.class,i);
		}
		public TerminalNode EOF() { return getToken(YdbMatViewV1Parser.EOF, 0); }
		public List<TerminalNode> SEMICOLON() { return getTokens(YdbMatViewV1Parser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(YdbMatViewV1Parser.SEMICOLON, i);
		}
		public Sql_scriptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_script; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterSql_script(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitSql_script(this);
		}
	}

	public final Sql_scriptContext sql_script() throws RecognitionException {
		Sql_scriptContext _localctx = new Sql_scriptContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_sql_script);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SEMICOLON) {
				{
				{
				setState(48);
				match(SEMICOLON);
				}
				}
				setState(53);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(54);
			sql_stmt();
			setState(63);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(56); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(55);
						match(SEMICOLON);
						}
						}
						setState(58); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==SEMICOLON );
					setState(60);
					sql_stmt();
					}
					} 
				}
				setState(65);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			}
			setState(69);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SEMICOLON) {
				{
				{
				setState(66);
				match(SEMICOLON);
				}
				}
				setState(71);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(72);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_stmtContext extends ParserRuleContext {
		public Create_mat_view_stmtContext create_mat_view_stmt() {
			return getRuleContext(Create_mat_view_stmtContext.class,0);
		}
		public Process_stmtContext process_stmt() {
			return getRuleContext(Process_stmtContext.class,0);
		}
		public Sql_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterSql_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitSql_stmt(this);
		}
	}

	public final Sql_stmtContext sql_stmt() throws RecognitionException {
		Sql_stmtContext _localctx = new Sql_stmtContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_sql_stmt);
		try {
			setState(76);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CREATE:
				enterOuterAlt(_localctx, 1);
				{
				setState(74);
				create_mat_view_stmt();
				}
				break;
			case PROCESS:
				enterOuterAlt(_localctx, 2);
				{
				setState(75);
				process_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_mat_view_stmtContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(YdbMatViewV1Parser.CREATE, 0); }
		public TerminalNode ASYNC() { return getToken(YdbMatViewV1Parser.ASYNC, 0); }
		public TerminalNode MATERIALIZED() { return getToken(YdbMatViewV1Parser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(YdbMatViewV1Parser.VIEW, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(YdbMatViewV1Parser.AS, 0); }
		public Simple_select_stmtContext simple_select_stmt() {
			return getRuleContext(Simple_select_stmtContext.class,0);
		}
		public Create_mat_view_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_mat_view_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterCreate_mat_view_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitCreate_mat_view_stmt(this);
		}
	}

	public final Create_mat_view_stmtContext create_mat_view_stmt() throws RecognitionException {
		Create_mat_view_stmtContext _localctx = new Create_mat_view_stmtContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_create_mat_view_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(78);
			match(CREATE);
			setState(79);
			match(ASYNC);
			setState(80);
			match(MATERIALIZED);
			setState(81);
			match(VIEW);
			setState(82);
			identifier();
			setState(83);
			match(AS);
			setState(84);
			simple_select_stmt();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Process_stmtContext extends ParserRuleContext {
		public TerminalNode PROCESS() { return getToken(YdbMatViewV1Parser.PROCESS, 0); }
		public Main_table_refContext main_table_ref() {
			return getRuleContext(Main_table_refContext.class,0);
		}
		public TerminalNode CHANGEFEED() { return getToken(YdbMatViewV1Parser.CHANGEFEED, 0); }
		public Changefeed_nameContext changefeed_name() {
			return getRuleContext(Changefeed_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(YdbMatViewV1Parser.AS, 0); }
		public TerminalNode STREAM() { return getToken(YdbMatViewV1Parser.STREAM, 0); }
		public TerminalNode BATCH() { return getToken(YdbMatViewV1Parser.BATCH, 0); }
		public Process_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_process_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterProcess_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitProcess_stmt(this);
		}
	}

	public final Process_stmtContext process_stmt() throws RecognitionException {
		Process_stmtContext _localctx = new Process_stmtContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_process_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(86);
			match(PROCESS);
			setState(87);
			main_table_ref();
			setState(88);
			match(CHANGEFEED);
			setState(89);
			changefeed_name();
			setState(90);
			match(AS);
			setState(91);
			_la = _input.LA(1);
			if ( !(_la==BATCH || _la==STREAM) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Simple_select_stmtContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(YdbMatViewV1Parser.SELECT, 0); }
		public List<Result_columnContext> result_column() {
			return getRuleContexts(Result_columnContext.class);
		}
		public Result_columnContext result_column(int i) {
			return getRuleContext(Result_columnContext.class,i);
		}
		public TerminalNode FROM() { return getToken(YdbMatViewV1Parser.FROM, 0); }
		public Main_table_refContext main_table_ref() {
			return getRuleContext(Main_table_refContext.class,0);
		}
		public TerminalNode AS() { return getToken(YdbMatViewV1Parser.AS, 0); }
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(YdbMatViewV1Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(YdbMatViewV1Parser.COMMA, i);
		}
		public List<Simple_join_partContext> simple_join_part() {
			return getRuleContexts(Simple_join_partContext.class);
		}
		public Simple_join_partContext simple_join_part(int i) {
			return getRuleContext(Simple_join_partContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(YdbMatViewV1Parser.WHERE, 0); }
		public Opaque_expressionContext opaque_expression() {
			return getRuleContext(Opaque_expressionContext.class,0);
		}
		public Simple_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterSimple_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitSimple_select_stmt(this);
		}
	}

	public final Simple_select_stmtContext simple_select_stmt() throws RecognitionException {
		Simple_select_stmtContext _localctx = new Simple_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_simple_select_stmt);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(93);
			match(SELECT);
			setState(94);
			result_column();
			setState(99);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(95);
					match(COMMA);
					setState(96);
					result_column();
					}
					} 
				}
				setState(101);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
			}
			setState(103);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(102);
				match(COMMA);
				}
			}

			setState(105);
			match(FROM);
			setState(106);
			main_table_ref();
			setState(107);
			match(AS);
			setState(108);
			table_alias();
			setState(112);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << JOIN) | (1L << INNER) | (1L << LEFT))) != 0)) {
				{
				{
				setState(109);
				simple_join_part();
				}
				}
				setState(114);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(117);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(115);
				match(WHERE);
				setState(116);
				opaque_expression();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Simple_join_partContext extends ParserRuleContext {
		public TerminalNode JOIN() { return getToken(YdbMatViewV1Parser.JOIN, 0); }
		public Join_table_refContext join_table_ref() {
			return getRuleContext(Join_table_refContext.class,0);
		}
		public TerminalNode AS() { return getToken(YdbMatViewV1Parser.AS, 0); }
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode ON() { return getToken(YdbMatViewV1Parser.ON, 0); }
		public List<Join_conditionContext> join_condition() {
			return getRuleContexts(Join_conditionContext.class);
		}
		public Join_conditionContext join_condition(int i) {
			return getRuleContext(Join_conditionContext.class,i);
		}
		public TerminalNode INNER() { return getToken(YdbMatViewV1Parser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(YdbMatViewV1Parser.LEFT, 0); }
		public List<TerminalNode> AND() { return getTokens(YdbMatViewV1Parser.AND); }
		public TerminalNode AND(int i) {
			return getToken(YdbMatViewV1Parser.AND, i);
		}
		public TerminalNode OUTER() { return getToken(YdbMatViewV1Parser.OUTER, 0); }
		public Simple_join_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_join_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterSimple_join_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitSimple_join_part(this);
		}
	}

	public final Simple_join_partContext simple_join_part() throws RecognitionException {
		Simple_join_partContext _localctx = new Simple_join_partContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_simple_join_part);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(124);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
				{
				setState(119);
				match(INNER);
				}
				break;
			case LEFT:
				{
				setState(120);
				match(LEFT);
				setState(122);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(121);
					match(OUTER);
					}
				}

				}
				break;
			case JOIN:
				break;
			default:
				break;
			}
			setState(126);
			match(JOIN);
			setState(127);
			join_table_ref();
			setState(128);
			match(AS);
			setState(129);
			table_alias();
			setState(130);
			match(ON);
			setState(131);
			join_condition();
			setState(136);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==AND) {
				{
				{
				setState(132);
				match(AND);
				setState(133);
				join_condition();
				}
				}
				setState(138);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Result_columnContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(YdbMatViewV1Parser.AS, 0); }
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public Column_referenceContext column_reference() {
			return getRuleContext(Column_referenceContext.class,0);
		}
		public Opaque_expressionContext opaque_expression() {
			return getRuleContext(Opaque_expressionContext.class,0);
		}
		public Result_columnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_result_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterResult_column(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitResult_column(this);
		}
	}

	public final Result_columnContext result_column() throws RecognitionException {
		Result_columnContext _localctx = new Result_columnContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_result_column);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID_PLAIN:
				{
				setState(139);
				column_reference();
				}
				break;
			case COMPUTE:
				{
				setState(140);
				opaque_expression();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(143);
			match(AS);
			setState(144);
			column_alias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Opaque_expressionContext extends ParserRuleContext {
		public TerminalNode COMPUTE() { return getToken(YdbMatViewV1Parser.COMPUTE, 0); }
		public Opaque_expression_bodyContext opaque_expression_body() {
			return getRuleContext(Opaque_expression_bodyContext.class,0);
		}
		public TerminalNode ON() { return getToken(YdbMatViewV1Parser.ON, 0); }
		public List<Table_aliasContext> table_alias() {
			return getRuleContexts(Table_aliasContext.class);
		}
		public Table_aliasContext table_alias(int i) {
			return getRuleContext(Table_aliasContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(YdbMatViewV1Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(YdbMatViewV1Parser.COMMA, i);
		}
		public Opaque_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_opaque_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterOpaque_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitOpaque_expression(this);
		}
	}

	public final Opaque_expressionContext opaque_expression() throws RecognitionException {
		Opaque_expressionContext _localctx = new Opaque_expressionContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_opaque_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(146);
			match(COMPUTE);
			setState(156);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(147);
				match(ON);
				setState(148);
				table_alias();
				setState(153);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(149);
					match(COMMA);
					setState(150);
					table_alias();
					}
					}
					setState(155);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(158);
			opaque_expression_body();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Opaque_expression_bodyContext extends ParserRuleContext {
		public TerminalNode OPAQUE_EXPRESSION() { return getToken(YdbMatViewV1Parser.OPAQUE_EXPRESSION, 0); }
		public Opaque_expression_bodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_opaque_expression_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterOpaque_expression_body(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitOpaque_expression_body(this);
		}
	}

	public final Opaque_expression_bodyContext opaque_expression_body() throws RecognitionException {
		Opaque_expression_bodyContext _localctx = new Opaque_expression_bodyContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_opaque_expression_body);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(160);
			match(OPAQUE_EXPRESSION);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_conditionContext extends ParserRuleContext {
		public TerminalNode EQUALS() { return getToken(YdbMatViewV1Parser.EQUALS, 0); }
		public Column_reference_firstContext column_reference_first() {
			return getRuleContext(Column_reference_firstContext.class,0);
		}
		public Constant_firstContext constant_first() {
			return getRuleContext(Constant_firstContext.class,0);
		}
		public Column_reference_secondContext column_reference_second() {
			return getRuleContext(Column_reference_secondContext.class,0);
		}
		public Constant_secondContext constant_second() {
			return getRuleContext(Constant_secondContext.class,0);
		}
		public Join_conditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_condition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterJoin_condition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitJoin_condition(this);
		}
	}

	public final Join_conditionContext join_condition() throws RecognitionException {
		Join_conditionContext _localctx = new Join_conditionContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_join_condition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(164);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID_PLAIN:
				{
				setState(162);
				column_reference_first();
				}
				break;
			case MINUS:
			case QUOTE_SINGLE:
			case DIGITS:
				{
				setState(163);
				constant_first();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(166);
			match(EQUALS);
			setState(169);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID_PLAIN:
				{
				setState(167);
				column_reference_second();
				}
				break;
			case MINUS:
			case QUOTE_SINGLE:
			case DIGITS:
				{
				setState(168);
				constant_second();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_reference_firstContext extends ParserRuleContext {
		public Column_referenceContext column_reference() {
			return getRuleContext(Column_referenceContext.class,0);
		}
		public Column_reference_firstContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_reference_first; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterColumn_reference_first(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitColumn_reference_first(this);
		}
	}

	public final Column_reference_firstContext column_reference_first() throws RecognitionException {
		Column_reference_firstContext _localctx = new Column_reference_firstContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_column_reference_first);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(171);
			column_reference();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_reference_secondContext extends ParserRuleContext {
		public Column_referenceContext column_reference() {
			return getRuleContext(Column_referenceContext.class,0);
		}
		public Column_reference_secondContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_reference_second; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterColumn_reference_second(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitColumn_reference_second(this);
		}
	}

	public final Column_reference_secondContext column_reference_second() throws RecognitionException {
		Column_reference_secondContext _localctx = new Column_reference_secondContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_column_reference_second);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(173);
			column_reference();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Constant_firstContext extends ParserRuleContext {
		public Integer_constantContext integer_constant() {
			return getRuleContext(Integer_constantContext.class,0);
		}
		public String_constantContext string_constant() {
			return getRuleContext(String_constantContext.class,0);
		}
		public Constant_firstContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant_first; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterConstant_first(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitConstant_first(this);
		}
	}

	public final Constant_firstContext constant_first() throws RecognitionException {
		Constant_firstContext _localctx = new Constant_firstContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_constant_first);
		try {
			setState(177);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
			case DIGITS:
				enterOuterAlt(_localctx, 1);
				{
				setState(175);
				integer_constant();
				}
				break;
			case QUOTE_SINGLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(176);
				string_constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Constant_secondContext extends ParserRuleContext {
		public Integer_constantContext integer_constant() {
			return getRuleContext(Integer_constantContext.class,0);
		}
		public String_constantContext string_constant() {
			return getRuleContext(String_constantContext.class,0);
		}
		public Constant_secondContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant_second; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterConstant_second(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitConstant_second(this);
		}
	}

	public final Constant_secondContext constant_second() throws RecognitionException {
		Constant_secondContext _localctx = new Constant_secondContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_constant_second);
		try {
			setState(181);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
			case DIGITS:
				enterOuterAlt(_localctx, 1);
				{
				setState(179);
				integer_constant();
				}
				break;
			case QUOTE_SINGLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(180);
				string_constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Integer_constantContext extends ParserRuleContext {
		public TerminalNode DIGITS() { return getToken(YdbMatViewV1Parser.DIGITS, 0); }
		public TerminalNode MINUS() { return getToken(YdbMatViewV1Parser.MINUS, 0); }
		public Integer_constantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integer_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterInteger_constant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitInteger_constant(this);
		}
	}

	public final Integer_constantContext integer_constant() throws RecognitionException {
		Integer_constantContext _localctx = new Integer_constantContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_integer_constant);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(184);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MINUS) {
				{
				setState(183);
				match(MINUS);
				}
			}

			setState(186);
			match(DIGITS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class String_constantContext extends ParserRuleContext {
		public List<TerminalNode> QUOTE_SINGLE() { return getTokens(YdbMatViewV1Parser.QUOTE_SINGLE); }
		public TerminalNode QUOTE_SINGLE(int i) {
			return getToken(YdbMatViewV1Parser.QUOTE_SINGLE, i);
		}
		public String_constantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterString_constant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitString_constant(this);
		}
	}

	public final String_constantContext string_constant() throws RecognitionException {
		String_constantContext _localctx = new String_constantContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_string_constant);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(188);
			match(QUOTE_SINGLE);
			setState(192); 
			_errHandler.sync(this);
			_alt = 1+1;
			do {
				switch (_alt) {
				case 1+1:
					{
					setState(192);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case OPAQUE_EXPRESSION:
					case AND:
					case AS:
					case ASYNC:
					case BATCH:
					case CHANGEFEED:
					case COMPUTE:
					case CREATE:
					case FROM:
					case JOIN:
					case INNER:
					case LEFT:
					case MATERIALIZED:
					case ON:
					case OUTER:
					case PROCESS:
					case SELECT:
					case STREAM:
					case VIEW:
					case WHERE:
					case SEMICOLON:
					case COMMA:
					case DOT:
					case MINUS:
					case EQUALS:
					case DIGITS:
					case ID_PLAIN:
					case ID_QUOTED:
					case COMMENT:
					case WS:
						{
						setState(189);
						_la = _input.LA(1);
						if ( _la <= 0 || (_la==T__0 || _la==QUOTE_SINGLE) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						}
						break;
					case T__0:
						{
						{
						setState(190);
						match(T__0);
						setState(191);
						matchWildcard();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(194); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,21,_ctx);
			} while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			setState(196);
			match(QUOTE_SINGLE);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_referenceContext extends ParserRuleContext {
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode DOT() { return getToken(YdbMatViewV1Parser.DOT, 0); }
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Column_referenceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_reference; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterColumn_reference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitColumn_reference(this);
		}
	}

	public final Column_referenceContext column_reference() throws RecognitionException {
		Column_referenceContext _localctx = new Column_referenceContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_column_reference);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(198);
			table_alias();
			setState(199);
			match(DOT);
			setState(200);
			column_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitColumn_name(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(202);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Main_table_refContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Main_table_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_main_table_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterMain_table_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitMain_table_ref(this);
		}
	}

	public final Main_table_refContext main_table_ref() throws RecognitionException {
		Main_table_refContext _localctx = new Main_table_refContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_main_table_ref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(204);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_table_refContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Join_table_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_table_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterJoin_table_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitJoin_table_ref(this);
		}
	}

	public final Join_table_refContext join_table_ref() throws RecognitionException {
		Join_table_refContext _localctx = new Join_table_refContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_join_table_ref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(206);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Changefeed_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Changefeed_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_changefeed_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterChangefeed_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitChangefeed_name(this);
		}
	}

	public final Changefeed_nameContext changefeed_name() throws RecognitionException {
		Changefeed_nameContext _localctx = new Changefeed_nameContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_changefeed_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(208);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_aliasContext extends ParserRuleContext {
		public TerminalNode ID_PLAIN() { return getToken(YdbMatViewV1Parser.ID_PLAIN, 0); }
		public Table_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterTable_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitTable_alias(this);
		}
	}

	public final Table_aliasContext table_alias() throws RecognitionException {
		Table_aliasContext _localctx = new Table_aliasContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_table_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210);
			match(ID_PLAIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_aliasContext extends ParserRuleContext {
		public TerminalNode ID_PLAIN() { return getToken(YdbMatViewV1Parser.ID_PLAIN, 0); }
		public Column_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterColumn_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitColumn_alias(this);
		}
	}

	public final Column_aliasContext column_alias() throws RecognitionException {
		Column_aliasContext _localctx = new Column_aliasContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_column_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(212);
			match(ID_PLAIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode ID_PLAIN() { return getToken(YdbMatViewV1Parser.ID_PLAIN, 0); }
		public TerminalNode ID_QUOTED() { return getToken(YdbMatViewV1Parser.ID_QUOTED, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof YdbMatViewV1Listener ) ((YdbMatViewV1Listener)listener).exitIdentifier(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(214);
			_la = _input.LA(1);
			if ( !(_la==ID_PLAIN || _la==ID_QUOTED) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\"\u00db\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\3\2\7\2\64\n\2\f\2\16\2\67\13\2\3\2\3\2\6\2;\n\2\r\2\16\2<\3\2\7\2@\n"+
		"\2\f\2\16\2C\13\2\3\2\7\2F\n\2\f\2\16\2I\13\2\3\2\3\2\3\3\3\3\5\3O\n\3"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3"+
		"\6\3\6\7\6d\n\6\f\6\16\6g\13\6\3\6\5\6j\n\6\3\6\3\6\3\6\3\6\3\6\7\6q\n"+
		"\6\f\6\16\6t\13\6\3\6\3\6\5\6x\n\6\3\7\3\7\3\7\5\7}\n\7\5\7\177\n\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u0089\n\7\f\7\16\7\u008c\13\7\3\b\3"+
		"\b\5\b\u0090\n\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\7\t\u009a\n\t\f\t\16"+
		"\t\u009d\13\t\5\t\u009f\n\t\3\t\3\t\3\n\3\n\3\13\3\13\5\13\u00a7\n\13"+
		"\3\13\3\13\3\13\5\13\u00ac\n\13\3\f\3\f\3\r\3\r\3\16\3\16\5\16\u00b4\n"+
		"\16\3\17\3\17\5\17\u00b8\n\17\3\20\5\20\u00bb\n\20\3\20\3\20\3\21\3\21"+
		"\3\21\3\21\6\21\u00c3\n\21\r\21\16\21\u00c4\3\21\3\21\3\22\3\22\3\22\3"+
		"\22\3\23\3\23\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\31\3"+
		"\31\3\31\3\u00c4\2\32\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,."+
		"\60\2\5\4\2\b\b\25\25\4\2\3\3\35\35\3\2\37 \2\u00d9\2\65\3\2\2\2\4N\3"+
		"\2\2\2\6P\3\2\2\2\bX\3\2\2\2\n_\3\2\2\2\f~\3\2\2\2\16\u008f\3\2\2\2\20"+
		"\u0094\3\2\2\2\22\u00a2\3\2\2\2\24\u00a6\3\2\2\2\26\u00ad\3\2\2\2\30\u00af"+
		"\3\2\2\2\32\u00b3\3\2\2\2\34\u00b7\3\2\2\2\36\u00ba\3\2\2\2 \u00be\3\2"+
		"\2\2\"\u00c8\3\2\2\2$\u00cc\3\2\2\2&\u00ce\3\2\2\2(\u00d0\3\2\2\2*\u00d2"+
		"\3\2\2\2,\u00d4\3\2\2\2.\u00d6\3\2\2\2\60\u00d8\3\2\2\2\62\64\7\30\2\2"+
		"\63\62\3\2\2\2\64\67\3\2\2\2\65\63\3\2\2\2\65\66\3\2\2\2\668\3\2\2\2\67"+
		"\65\3\2\2\28A\5\4\3\29;\7\30\2\2:9\3\2\2\2;<\3\2\2\2<:\3\2\2\2<=\3\2\2"+
		"\2=>\3\2\2\2>@\5\4\3\2?:\3\2\2\2@C\3\2\2\2A?\3\2\2\2AB\3\2\2\2BG\3\2\2"+
		"\2CA\3\2\2\2DF\7\30\2\2ED\3\2\2\2FI\3\2\2\2GE\3\2\2\2GH\3\2\2\2HJ\3\2"+
		"\2\2IG\3\2\2\2JK\7\2\2\3K\3\3\2\2\2LO\5\6\4\2MO\5\b\5\2NL\3\2\2\2NM\3"+
		"\2\2\2O\5\3\2\2\2PQ\7\13\2\2QR\7\7\2\2RS\7\20\2\2ST\7\26\2\2TU\5\60\31"+
		"\2UV\7\6\2\2VW\5\n\6\2W\7\3\2\2\2XY\7\23\2\2YZ\5&\24\2Z[\7\t\2\2[\\\5"+
		"*\26\2\\]\7\6\2\2]^\t\2\2\2^\t\3\2\2\2_`\7\24\2\2`e\5\16\b\2ab\7\31\2"+
		"\2bd\5\16\b\2ca\3\2\2\2dg\3\2\2\2ec\3\2\2\2ef\3\2\2\2fi\3\2\2\2ge\3\2"+
		"\2\2hj\7\31\2\2ih\3\2\2\2ij\3\2\2\2jk\3\2\2\2kl\7\f\2\2lm\5&\24\2mn\7"+
		"\6\2\2nr\5,\27\2oq\5\f\7\2po\3\2\2\2qt\3\2\2\2rp\3\2\2\2rs\3\2\2\2sw\3"+
		"\2\2\2tr\3\2\2\2uv\7\27\2\2vx\5\20\t\2wu\3\2\2\2wx\3\2\2\2x\13\3\2\2\2"+
		"y\177\7\16\2\2z|\7\17\2\2{}\7\22\2\2|{\3\2\2\2|}\3\2\2\2}\177\3\2\2\2"+
		"~y\3\2\2\2~z\3\2\2\2~\177\3\2\2\2\177\u0080\3\2\2\2\u0080\u0081\7\r\2"+
		"\2\u0081\u0082\5(\25\2\u0082\u0083\7\6\2\2\u0083\u0084\5,\27\2\u0084\u0085"+
		"\7\21\2\2\u0085\u008a\5\24\13\2\u0086\u0087\7\5\2\2\u0087\u0089\5\24\13"+
		"\2\u0088\u0086\3\2\2\2\u0089\u008c\3\2\2\2\u008a\u0088\3\2\2\2\u008a\u008b"+
		"\3\2\2\2\u008b\r\3\2\2\2\u008c\u008a\3\2\2\2\u008d\u0090\5\"\22\2\u008e"+
		"\u0090\5\20\t\2\u008f\u008d\3\2\2\2\u008f\u008e\3\2\2\2\u0090\u0091\3"+
		"\2\2\2\u0091\u0092\7\6\2\2\u0092\u0093\5.\30\2\u0093\17\3\2\2\2\u0094"+
		"\u009e\7\n\2\2\u0095\u0096\7\21\2\2\u0096\u009b\5,\27\2\u0097\u0098\7"+
		"\31\2\2\u0098\u009a\5,\27\2\u0099\u0097\3\2\2\2\u009a\u009d\3\2\2\2\u009b"+
		"\u0099\3\2\2\2\u009b\u009c\3\2\2\2\u009c\u009f\3\2\2\2\u009d\u009b\3\2"+
		"\2\2\u009e\u0095\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0"+
		"\u00a1\5\22\n\2\u00a1\21\3\2\2\2\u00a2\u00a3\7\4\2\2\u00a3\23\3\2\2\2"+
		"\u00a4\u00a7\5\26\f\2\u00a5\u00a7\5\32\16\2\u00a6\u00a4\3\2\2\2\u00a6"+
		"\u00a5\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\u00ab\7\34\2\2\u00a9\u00ac\5"+
		"\30\r\2\u00aa\u00ac\5\34\17\2\u00ab\u00a9\3\2\2\2\u00ab\u00aa\3\2\2\2"+
		"\u00ac\25\3\2\2\2\u00ad\u00ae\5\"\22\2\u00ae\27\3\2\2\2\u00af\u00b0\5"+
		"\"\22\2\u00b0\31\3\2\2\2\u00b1\u00b4\5\36\20\2\u00b2\u00b4\5 \21\2\u00b3"+
		"\u00b1\3\2\2\2\u00b3\u00b2\3\2\2\2\u00b4\33\3\2\2\2\u00b5\u00b8\5\36\20"+
		"\2\u00b6\u00b8\5 \21\2\u00b7\u00b5\3\2\2\2\u00b7\u00b6\3\2\2\2\u00b8\35"+
		"\3\2\2\2\u00b9\u00bb\7\33\2\2\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2"+
		"\u00bb\u00bc\3\2\2\2\u00bc\u00bd\7\36\2\2\u00bd\37\3\2\2\2\u00be\u00c2"+
		"\7\35\2\2\u00bf\u00c3\n\3\2\2\u00c0\u00c1\7\3\2\2\u00c1\u00c3\13\2\2\2"+
		"\u00c2\u00bf\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c5"+
		"\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\7\35\2\2"+
		"\u00c7!\3\2\2\2\u00c8\u00c9\5,\27\2\u00c9\u00ca\7\32\2\2\u00ca\u00cb\5"+
		"$\23\2\u00cb#\3\2\2\2\u00cc\u00cd\5\60\31\2\u00cd%\3\2\2\2\u00ce\u00cf"+
		"\5\60\31\2\u00cf\'\3\2\2\2\u00d0\u00d1\5\60\31\2\u00d1)\3\2\2\2\u00d2"+
		"\u00d3\5\60\31\2\u00d3+\3\2\2\2\u00d4\u00d5\7\37\2\2\u00d5-\3\2\2\2\u00d6"+
		"\u00d7\7\37\2\2\u00d7/\3\2\2\2\u00d8\u00d9\t\4\2\2\u00d9\61\3\2\2\2\30"+
		"\65<AGNeirw|~\u008a\u008f\u009b\u009e\u00a6\u00ab\u00b3\u00b7\u00ba\u00c2"+
		"\u00c4";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}