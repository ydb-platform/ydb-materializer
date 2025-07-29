// Generated from /home/zinal/Projects/YDB/integrations/ydb-materializer/src/main/antlr4/tech/ydb/mv/parser/YdbMatViewV1.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link YdbMatViewV1Parser}.
 */
public interface YdbMatViewV1Listener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#sql_script}.
	 * @param ctx the parse tree
	 */
	void enterSql_script(YdbMatViewV1Parser.Sql_scriptContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#sql_script}.
	 * @param ctx the parse tree
	 */
	void exitSql_script(YdbMatViewV1Parser.Sql_scriptContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#sql_stmt}.
	 * @param ctx the parse tree
	 */
	void enterSql_stmt(YdbMatViewV1Parser.Sql_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#sql_stmt}.
	 * @param ctx the parse tree
	 */
	void exitSql_stmt(YdbMatViewV1Parser.Sql_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#create_mat_view_stmt}.
	 * @param ctx the parse tree
	 */
	void enterCreate_mat_view_stmt(YdbMatViewV1Parser.Create_mat_view_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#create_mat_view_stmt}.
	 * @param ctx the parse tree
	 */
	void exitCreate_mat_view_stmt(YdbMatViewV1Parser.Create_mat_view_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#process_stmt}.
	 * @param ctx the parse tree
	 */
	void enterProcess_stmt(YdbMatViewV1Parser.Process_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#process_stmt}.
	 * @param ctx the parse tree
	 */
	void exitProcess_stmt(YdbMatViewV1Parser.Process_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#simple_select_stmt}.
	 * @param ctx the parse tree
	 */
	void enterSimple_select_stmt(YdbMatViewV1Parser.Simple_select_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#simple_select_stmt}.
	 * @param ctx the parse tree
	 */
	void exitSimple_select_stmt(YdbMatViewV1Parser.Simple_select_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#simple_join_part}.
	 * @param ctx the parse tree
	 */
	void enterSimple_join_part(YdbMatViewV1Parser.Simple_join_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#simple_join_part}.
	 * @param ctx the parse tree
	 */
	void exitSimple_join_part(YdbMatViewV1Parser.Simple_join_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#result_column}.
	 * @param ctx the parse tree
	 */
	void enterResult_column(YdbMatViewV1Parser.Result_columnContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#result_column}.
	 * @param ctx the parse tree
	 */
	void exitResult_column(YdbMatViewV1Parser.Result_columnContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#opaque_expression}.
	 * @param ctx the parse tree
	 */
	void enterOpaque_expression(YdbMatViewV1Parser.Opaque_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#opaque_expression}.
	 * @param ctx the parse tree
	 */
	void exitOpaque_expression(YdbMatViewV1Parser.Opaque_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#opaque_expression_body}.
	 * @param ctx the parse tree
	 */
	void enterOpaque_expression_body(YdbMatViewV1Parser.Opaque_expression_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#opaque_expression_body}.
	 * @param ctx the parse tree
	 */
	void exitOpaque_expression_body(YdbMatViewV1Parser.Opaque_expression_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#join_condition}.
	 * @param ctx the parse tree
	 */
	void enterJoin_condition(YdbMatViewV1Parser.Join_conditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#join_condition}.
	 * @param ctx the parse tree
	 */
	void exitJoin_condition(YdbMatViewV1Parser.Join_conditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#column_reference_first}.
	 * @param ctx the parse tree
	 */
	void enterColumn_reference_first(YdbMatViewV1Parser.Column_reference_firstContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#column_reference_first}.
	 * @param ctx the parse tree
	 */
	void exitColumn_reference_first(YdbMatViewV1Parser.Column_reference_firstContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#column_reference_second}.
	 * @param ctx the parse tree
	 */
	void enterColumn_reference_second(YdbMatViewV1Parser.Column_reference_secondContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#column_reference_second}.
	 * @param ctx the parse tree
	 */
	void exitColumn_reference_second(YdbMatViewV1Parser.Column_reference_secondContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#constant_first}.
	 * @param ctx the parse tree
	 */
	void enterConstant_first(YdbMatViewV1Parser.Constant_firstContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#constant_first}.
	 * @param ctx the parse tree
	 */
	void exitConstant_first(YdbMatViewV1Parser.Constant_firstContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#constant_second}.
	 * @param ctx the parse tree
	 */
	void enterConstant_second(YdbMatViewV1Parser.Constant_secondContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#constant_second}.
	 * @param ctx the parse tree
	 */
	void exitConstant_second(YdbMatViewV1Parser.Constant_secondContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#integer_constant}.
	 * @param ctx the parse tree
	 */
	void enterInteger_constant(YdbMatViewV1Parser.Integer_constantContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#integer_constant}.
	 * @param ctx the parse tree
	 */
	void exitInteger_constant(YdbMatViewV1Parser.Integer_constantContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#string_constant}.
	 * @param ctx the parse tree
	 */
	void enterString_constant(YdbMatViewV1Parser.String_constantContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#string_constant}.
	 * @param ctx the parse tree
	 */
	void exitString_constant(YdbMatViewV1Parser.String_constantContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#column_reference}.
	 * @param ctx the parse tree
	 */
	void enterColumn_reference(YdbMatViewV1Parser.Column_referenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#column_reference}.
	 * @param ctx the parse tree
	 */
	void exitColumn_reference(YdbMatViewV1Parser.Column_referenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#column_name}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name(YdbMatViewV1Parser.Column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#column_name}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name(YdbMatViewV1Parser.Column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#main_table_ref}.
	 * @param ctx the parse tree
	 */
	void enterMain_table_ref(YdbMatViewV1Parser.Main_table_refContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#main_table_ref}.
	 * @param ctx the parse tree
	 */
	void exitMain_table_ref(YdbMatViewV1Parser.Main_table_refContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#join_table_ref}.
	 * @param ctx the parse tree
	 */
	void enterJoin_table_ref(YdbMatViewV1Parser.Join_table_refContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#join_table_ref}.
	 * @param ctx the parse tree
	 */
	void exitJoin_table_ref(YdbMatViewV1Parser.Join_table_refContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#changefeed_name}.
	 * @param ctx the parse tree
	 */
	void enterChangefeed_name(YdbMatViewV1Parser.Changefeed_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#changefeed_name}.
	 * @param ctx the parse tree
	 */
	void exitChangefeed_name(YdbMatViewV1Parser.Changefeed_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#table_alias}.
	 * @param ctx the parse tree
	 */
	void enterTable_alias(YdbMatViewV1Parser.Table_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#table_alias}.
	 * @param ctx the parse tree
	 */
	void exitTable_alias(YdbMatViewV1Parser.Table_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#column_alias}.
	 * @param ctx the parse tree
	 */
	void enterColumn_alias(YdbMatViewV1Parser.Column_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#column_alias}.
	 * @param ctx the parse tree
	 */
	void exitColumn_alias(YdbMatViewV1Parser.Column_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link YdbMatViewV1Parser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(YdbMatViewV1Parser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link YdbMatViewV1Parser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(YdbMatViewV1Parser.IdentifierContext ctx);
}