// Generated from /home/zinal/Projects/YDB/integrations/ydb-materializer/src/main/antlr4/tech/ydb/mv/parser/YdbMatViewV1.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class YdbMatViewV1Lexer extends Lexer {
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
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "OPAQUE_BEGIN", "OPAQUE_END", "OPAQUE_EXPRESSION", "AND", "AS", 
			"ASYNC", "BATCH", "CHANGEFEED", "COMPUTE", "CREATE", "FROM", "JOIN", 
			"INNER", "LEFT", "MATERIALIZED", "ON", "OUTER", "PROCESS", "SELECT", 
			"STREAM", "VIEW", "WHERE", "SEMICOLON", "COMMA", "DOT", "MINUS", "EQUALS", 
			"QUOTE_SINGLE", "DIGIT", "DIGITS", "BACKTICK", "ID_PLAIN", "ID_QUOTED", 
			"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", 
			"O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "MULTILINE_COMMENT", 
			"LINE_COMMENT", "COMMENT", "WS"
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


	public YdbMatViewV1Lexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "YdbMatViewV1.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\"\u0193\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\7\5\u008e"+
		"\n\5\f\5\16\5\u0091\13\5\3\5\3\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\b\3\b\3"+
		"\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3"+
		"\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25"+
		"\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27"+
		"\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34"+
		"\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \6 \u011d\n \r \16 \u011e\3!\3!"+
		"\3\"\3\"\3\"\7\"\u0126\n\"\f\"\16\"\u0129\13\"\3#\3#\3#\3#\3#\3#\6#\u0131"+
		"\n#\r#\16#\u0132\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+"+
		"\3+\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3"+
		"\64\3\64\3\65\3\65\3\66\3\66\3\67\3\67\38\38\39\39\3:\3:\3;\3;\3<\3<\3"+
		"=\3=\3>\3>\3>\3>\7>\u016f\n>\f>\16>\u0172\13>\3>\3>\3>\3?\3?\3?\3?\7?"+
		"\u017b\n?\f?\16?\u017e\13?\3?\3?\5?\u0182\n?\3?\5?\u0185\n?\3@\3@\5@\u0189"+
		"\n@\3@\3@\3A\6A\u018e\nA\rA\16A\u018f\3A\3A\5\u008f\u0132\u0170\2B\3\3"+
		"\5\2\7\2\t\4\13\5\r\6\17\7\21\b\23\t\25\n\27\13\31\f\33\r\35\16\37\17"+
		"!\20#\21%\22\'\23)\24+\25-\26/\27\61\30\63\31\65\32\67\339\34;\35=\2?"+
		"\36A\2C\37E G\2I\2K\2M\2O\2Q\2S\2U\2W\2Y\2[\2]\2_\2a\2c\2e\2g\2i\2k\2"+
		"m\2o\2q\2s\2u\2w\2y\2{\2}\2\177!\u0081\"\3\2!\5\2C\\aac|\4\2^^bb\4\2C"+
		"Ccc\4\2DDdd\4\2EEee\4\2FFff\4\2GGgg\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4"+
		"\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTt"+
		"t\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\||\4"+
		"\2\f\f\17\17\3\3\f\f\5\2\13\f\17\17\"\"\2\u017f\2\3\3\2\2\2\2\t\3\2\2"+
		"\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25"+
		"\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2"+
		"\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2"+
		"\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3"+
		"\2\2\2\29\3\2\2\2\2;\3\2\2\2\2?\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2\177\3"+
		"\2\2\2\2\u0081\3\2\2\2\3\u0083\3\2\2\2\5\u0085\3\2\2\2\7\u0088\3\2\2\2"+
		"\t\u008b\3\2\2\2\13\u0094\3\2\2\2\r\u0098\3\2\2\2\17\u009b\3\2\2\2\21"+
		"\u00a1\3\2\2\2\23\u00a7\3\2\2\2\25\u00b2\3\2\2\2\27\u00ba\3\2\2\2\31\u00c1"+
		"\3\2\2\2\33\u00c6\3\2\2\2\35\u00cb\3\2\2\2\37\u00d1\3\2\2\2!\u00d6\3\2"+
		"\2\2#\u00e3\3\2\2\2%\u00e6\3\2\2\2\'\u00ec\3\2\2\2)\u00f4\3\2\2\2+\u00fb"+
		"\3\2\2\2-\u0102\3\2\2\2/\u0107\3\2\2\2\61\u010d\3\2\2\2\63\u010f\3\2\2"+
		"\2\65\u0111\3\2\2\2\67\u0113\3\2\2\29\u0115\3\2\2\2;\u0117\3\2\2\2=\u0119"+
		"\3\2\2\2?\u011c\3\2\2\2A\u0120\3\2\2\2C\u0122\3\2\2\2E\u012a\3\2\2\2G"+
		"\u0136\3\2\2\2I\u0138\3\2\2\2K\u013a\3\2\2\2M\u013c\3\2\2\2O\u013e\3\2"+
		"\2\2Q\u0140\3\2\2\2S\u0142\3\2\2\2U\u0144\3\2\2\2W\u0146\3\2\2\2Y\u0148"+
		"\3\2\2\2[\u014a\3\2\2\2]\u014c\3\2\2\2_\u014e\3\2\2\2a\u0150\3\2\2\2c"+
		"\u0152\3\2\2\2e\u0154\3\2\2\2g\u0156\3\2\2\2i\u0158\3\2\2\2k\u015a\3\2"+
		"\2\2m\u015c\3\2\2\2o\u015e\3\2\2\2q\u0160\3\2\2\2s\u0162\3\2\2\2u\u0164"+
		"\3\2\2\2w\u0166\3\2\2\2y\u0168\3\2\2\2{\u016a\3\2\2\2}\u0176\3\2\2\2\177"+
		"\u0188\3\2\2\2\u0081\u018d\3\2\2\2\u0083\u0084\7^\2\2\u0084\4\3\2\2\2"+
		"\u0085\u0086\7%\2\2\u0086\u0087\7]\2\2\u0087\6\3\2\2\2\u0088\u0089\7_"+
		"\2\2\u0089\u008a\7%\2\2\u008a\b\3\2\2\2\u008b\u008f\5\5\3\2\u008c\u008e"+
		"\13\2\2\2\u008d\u008c\3\2\2\2\u008e\u0091\3\2\2\2\u008f\u0090\3\2\2\2"+
		"\u008f\u008d\3\2\2\2\u0090\u0092\3\2\2\2\u0091\u008f\3\2\2\2\u0092\u0093"+
		"\5\7\4\2\u0093\n\3\2\2\2\u0094\u0095\5G$\2\u0095\u0096\5a\61\2\u0096\u0097"+
		"\5M\'\2\u0097\f\3\2\2\2\u0098\u0099\5G$\2\u0099\u009a\5k\66\2\u009a\16"+
		"\3\2\2\2\u009b\u009c\5G$\2\u009c\u009d\5k\66\2\u009d\u009e\5w<\2\u009e"+
		"\u009f\5a\61\2\u009f\u00a0\5K&\2\u00a0\20\3\2\2\2\u00a1\u00a2\5I%\2\u00a2"+
		"\u00a3\5G$\2\u00a3\u00a4\5m\67\2\u00a4\u00a5\5K&\2\u00a5\u00a6\5U+\2\u00a6"+
		"\22\3\2\2\2\u00a7\u00a8\5K&\2\u00a8\u00a9\5U+\2\u00a9\u00aa\5G$\2\u00aa"+
		"\u00ab\5a\61\2\u00ab\u00ac\5S*\2\u00ac\u00ad\5O(\2\u00ad\u00ae\5Q)\2\u00ae"+
		"\u00af\5O(\2\u00af\u00b0\5O(\2\u00b0\u00b1\5M\'\2\u00b1\24\3\2\2\2\u00b2"+
		"\u00b3\5K&\2\u00b3\u00b4\5c\62\2\u00b4\u00b5\5_\60\2\u00b5\u00b6\5e\63"+
		"\2\u00b6\u00b7\5o8\2\u00b7\u00b8\5m\67\2\u00b8\u00b9\5O(\2\u00b9\26\3"+
		"\2\2\2\u00ba\u00bb\5K&\2\u00bb\u00bc\5i\65\2\u00bc\u00bd\5O(\2\u00bd\u00be"+
		"\5G$\2\u00be\u00bf\5m\67\2\u00bf\u00c0\5O(\2\u00c0\30\3\2\2\2\u00c1\u00c2"+
		"\5Q)\2\u00c2\u00c3\5i\65\2\u00c3\u00c4\5c\62\2\u00c4\u00c5\5_\60\2\u00c5"+
		"\32\3\2\2\2\u00c6\u00c7\5Y-\2\u00c7\u00c8\5c\62\2\u00c8\u00c9\5W,\2\u00c9"+
		"\u00ca\5a\61\2\u00ca\34\3\2\2\2\u00cb\u00cc\5W,\2\u00cc\u00cd\5a\61\2"+
		"\u00cd\u00ce\5a\61\2\u00ce\u00cf\5O(\2\u00cf\u00d0\5i\65\2\u00d0\36\3"+
		"\2\2\2\u00d1\u00d2\5]/\2\u00d2\u00d3\5O(\2\u00d3\u00d4\5Q)\2\u00d4\u00d5"+
		"\5m\67\2\u00d5 \3\2\2\2\u00d6\u00d7\5_\60\2\u00d7\u00d8\5G$\2\u00d8\u00d9"+
		"\5m\67\2\u00d9\u00da\5O(\2\u00da\u00db\5i\65\2\u00db\u00dc\5W,\2\u00dc"+
		"\u00dd\5G$\2\u00dd\u00de\5]/\2\u00de\u00df\5W,\2\u00df\u00e0\5y=\2\u00e0"+
		"\u00e1\5O(\2\u00e1\u00e2\5M\'\2\u00e2\"\3\2\2\2\u00e3\u00e4\5c\62\2\u00e4"+
		"\u00e5\5a\61\2\u00e5$\3\2\2\2\u00e6\u00e7\5c\62\2\u00e7\u00e8\5o8\2\u00e8"+
		"\u00e9\5m\67\2\u00e9\u00ea\5O(\2\u00ea\u00eb\5i\65\2\u00eb&\3\2\2\2\u00ec"+
		"\u00ed\5e\63\2\u00ed\u00ee\5i\65\2\u00ee\u00ef\5c\62\2\u00ef\u00f0\5K"+
		"&\2\u00f0\u00f1\5O(\2\u00f1\u00f2\5k\66\2\u00f2\u00f3\5k\66\2\u00f3(\3"+
		"\2\2\2\u00f4\u00f5\5k\66\2\u00f5\u00f6\5O(\2\u00f6\u00f7\5]/\2\u00f7\u00f8"+
		"\5O(\2\u00f8\u00f9\5K&\2\u00f9\u00fa\5m\67\2\u00fa*\3\2\2\2\u00fb\u00fc"+
		"\5k\66\2\u00fc\u00fd\5m\67\2\u00fd\u00fe\5i\65\2\u00fe\u00ff\5O(\2\u00ff"+
		"\u0100\5G$\2\u0100\u0101\5_\60\2\u0101,\3\2\2\2\u0102\u0103\5q9\2\u0103"+
		"\u0104\5W,\2\u0104\u0105\5O(\2\u0105\u0106\5s:\2\u0106.\3\2\2\2\u0107"+
		"\u0108\5s:\2\u0108\u0109\5U+\2\u0109\u010a\5O(\2\u010a\u010b\5i\65\2\u010b"+
		"\u010c\5O(\2\u010c\60\3\2\2\2\u010d\u010e\7=\2\2\u010e\62\3\2\2\2\u010f"+
		"\u0110\7.\2\2\u0110\64\3\2\2\2\u0111\u0112\7\60\2\2\u0112\66\3\2\2\2\u0113"+
		"\u0114\7/\2\2\u01148\3\2\2\2\u0115\u0116\7?\2\2\u0116:\3\2\2\2\u0117\u0118"+
		"\7)\2\2\u0118<\3\2\2\2\u0119\u011a\4\62;\2\u011a>\3\2\2\2\u011b\u011d"+
		"\5=\37\2\u011c\u011b\3\2\2\2\u011d\u011e\3\2\2\2\u011e\u011c\3\2\2\2\u011e"+
		"\u011f\3\2\2\2\u011f@\3\2\2\2\u0120\u0121\7b\2\2\u0121B\3\2\2\2\u0122"+
		"\u0127\t\2\2\2\u0123\u0126\t\2\2\2\u0124\u0126\5=\37\2\u0125\u0123\3\2"+
		"\2\2\u0125\u0124\3\2\2\2\u0126\u0129\3\2\2\2\u0127\u0125\3\2\2\2\u0127"+
		"\u0128\3\2\2\2\u0128D\3\2\2\2\u0129\u0127\3\2\2\2\u012a\u0130\5A!\2\u012b"+
		"\u012c\7^\2\2\u012c\u0131\13\2\2\2\u012d\u012e\7b\2\2\u012e\u0131\7b\2"+
		"\2\u012f\u0131\n\3\2\2\u0130\u012b\3\2\2\2\u0130\u012d\3\2\2\2\u0130\u012f"+
		"\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0133\3\2\2\2\u0132\u0130\3\2\2\2\u0133"+
		"\u0134\3\2\2\2\u0134\u0135\5A!\2\u0135F\3\2\2\2\u0136\u0137\t\4\2\2\u0137"+
		"H\3\2\2\2\u0138\u0139\t\5\2\2\u0139J\3\2\2\2\u013a\u013b\t\6\2\2\u013b"+
		"L\3\2\2\2\u013c\u013d\t\7\2\2\u013dN\3\2\2\2\u013e\u013f\t\b\2\2\u013f"+
		"P\3\2\2\2\u0140\u0141\t\t\2\2\u0141R\3\2\2\2\u0142\u0143\t\n\2\2\u0143"+
		"T\3\2\2\2\u0144\u0145\t\13\2\2\u0145V\3\2\2\2\u0146\u0147\t\f\2\2\u0147"+
		"X\3\2\2\2\u0148\u0149\t\r\2\2\u0149Z\3\2\2\2\u014a\u014b\t\16\2\2\u014b"+
		"\\\3\2\2\2\u014c\u014d\t\17\2\2\u014d^\3\2\2\2\u014e\u014f\t\20\2\2\u014f"+
		"`\3\2\2\2\u0150\u0151\t\21\2\2\u0151b\3\2\2\2\u0152\u0153\t\22\2\2\u0153"+
		"d\3\2\2\2\u0154\u0155\t\23\2\2\u0155f\3\2\2\2\u0156\u0157\t\24\2\2\u0157"+
		"h\3\2\2\2\u0158\u0159\t\25\2\2\u0159j\3\2\2\2\u015a\u015b\t\26\2\2\u015b"+
		"l\3\2\2\2\u015c\u015d\t\27\2\2\u015dn\3\2\2\2\u015e\u015f\t\30\2\2\u015f"+
		"p\3\2\2\2\u0160\u0161\t\31\2\2\u0161r\3\2\2\2\u0162\u0163\t\32\2\2\u0163"+
		"t\3\2\2\2\u0164\u0165\t\33\2\2\u0165v\3\2\2\2\u0166\u0167\t\34\2\2\u0167"+
		"x\3\2\2\2\u0168\u0169\t\35\2\2\u0169z\3\2\2\2\u016a\u016b\7\61\2\2\u016b"+
		"\u016c\7,\2\2\u016c\u0170\3\2\2\2\u016d\u016f\13\2\2\2\u016e\u016d\3\2"+
		"\2\2\u016f\u0172\3\2\2\2\u0170\u0171\3\2\2\2\u0170\u016e\3\2\2\2\u0171"+
		"\u0173\3\2\2\2\u0172\u0170\3\2\2\2\u0173\u0174\7,\2\2\u0174\u0175\7\61"+
		"\2\2\u0175|\3\2\2\2\u0176\u0177\7/\2\2\u0177\u0178\7/\2\2\u0178\u017c"+
		"\3\2\2\2\u0179\u017b\n\36\2\2\u017a\u0179\3\2\2\2\u017b\u017e\3\2\2\2"+
		"\u017c\u017a\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u0184\3\2\2\2\u017e\u017c"+
		"\3\2\2\2\u017f\u0181\7\17\2\2\u0180\u0182\7\f\2\2\u0181\u0180\3\2\2\2"+
		"\u0181\u0182\3\2\2\2\u0182\u0185\3\2\2\2\u0183\u0185\t\37\2\2\u0184\u017f"+
		"\3\2\2\2\u0184\u0183\3\2\2\2\u0185~\3\2\2\2\u0186\u0189\5{>\2\u0187\u0189"+
		"\5}?\2\u0188\u0186\3\2\2\2\u0188\u0187\3\2\2\2\u0189\u018a\3\2\2\2\u018a"+
		"\u018b\b@\2\2\u018b\u0080\3\2\2\2\u018c\u018e\t \2\2\u018d\u018c\3\2\2"+
		"\2\u018e\u018f\3\2\2\2\u018f\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0191"+
		"\3\2\2\2\u0191\u0192\bA\2\2\u0192\u0082\3\2\2\2\17\2\u008f\u011e\u0125"+
		"\u0127\u0130\u0132\u0170\u017c\u0181\u0184\u0188\u018f\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}