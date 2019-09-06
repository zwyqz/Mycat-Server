package io.mycat.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.route.parser.druid.impl.DruidSelectParser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Hash Zhang on 2016/4/29.
 * Modified by Hash Zhang on 2016/5/25 add testGroupByWithViewAlias.
 */
public class DruidSelectParserTest {
    DruidSelectParser druidSelectParser = new DruidSelectParser();

    /**
     * 此方法检测DruidSelectParser的buildGroupByCols方法是否修改了函数列
     * 因为select的函数列并不做alias处理，
     * 所以在groupby也对函数列不做修改
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGroupByWithAlias() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String functionColumn = "DATE_FORMAT(h.times,'%b %d %Y %h:%i %p')";
        Object result = invokeGroupBy(functionColumn);
        Assert.assertEquals(functionColumn, ((String[]) result)[0]);
    }

    /**
     * 此方法检测DruidSelectParser对于子查询别名的全局解析
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGroupByWithViewAlias() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String functionColumn = "select id from (select h.id from hotnews h  union select h.title from hotnews h ) as t1 group by t1.id;";
        Object result = invokeGroupBy(functionColumn);
        Assert.assertEquals(functionColumn, ((String[]) result)[0]);
    }

    public Object invokeGroupBy(String functionColumn) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Map<String, String> aliaColumns = new TreeMap<>();
        SQLIdentifierExpr sqlExpr = mock(SQLIdentifierExpr.class);
        SQLIdentifierExpr expr = mock(SQLIdentifierExpr.class);
        List<SQLExpr> groupByItems = new ArrayList<>();
        groupByItems.add(sqlExpr);
        when((sqlExpr).getName()).thenReturn(functionColumn);
        Class c = DruidSelectParser.class;
        Method method = c.getDeclaredMethod("buildGroupByCols", new Class[]{List.class, Map.class});
        method.setAccessible(true);
        return  method.invoke(druidSelectParser, groupByItems, aliaColumns);
    }

    @Test
    public void testSubTable() {
        String sql  = "SELECT a.id, b.id, b.name FROM warehouse a LEFT JOIN travelrecord b ON a.id = b.id  WHERE b.name = '1'";
//        String sql  =     "SELECT a.id, a.warehouse_name FROM warehouse a  WHERE a.id = '44666670'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();

        SQLTableSource from = mysqlSelectQuery.getFrom();
        String orgTable = from.toString();
        SQLExpr where = mysqlSelectQuery.getWhere();
        List<SQLIdentifierExpr> exprs = new ArrayList<>(3);
        if (where != null){
            where.accept(new MySqlASTVisitorAdapter() {
                @Override
                public void endVisit(SQLIdentifierExpr x) {
                    if (orgTable.equalsIgnoreCase(x.getName())) {
                        exprs.add(x);
                    }
                    super.endVisit(x);
                }
            });
        }

//        SQLTableSource fromTable = ((SQLJoinTableSource)from).getLeft();
        List<SQLExprTableSource> subSqlTableSourceList = new ArrayList<>();
                getSubTableSourceList(from,"WAREHOUSE", subSqlTableSourceList);

        for (SQLExprTableSource sqlTableSource : subSqlTableSourceList) {
            SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
            sqlIdentifierExpr.setParent(from);
            sqlIdentifierExpr.setName("warehouse122111");
            sqlTableSource.setExpr(sqlIdentifierExpr);
        }
        System.out.println(stmt.toString());

//        SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
//        sqlIdentifierExpr.setParent(from);
//        sqlIdentifierExpr.setName("warehouse1");
//        SQLExprTableSource from2 = new SQLExprTableSource(sqlIdentifierExpr);
////		from2.setAlias(fromTable.getAlias());
//		from2.setAlias(from.getAlias());
//        mysqlSelectQuery.setFrom(from2);
//        ((SQLJoinTableSource)from).setLeft(from2);
//        for (SQLIdentifierExpr expr : exprs) {
//            expr.setName("warehouse1");
//        }
//        node.setStatement(stmt.toString());
    }

    private void getSubTableSourceList(SQLTableSource from, String subTableName, List<SQLExprTableSource> subSqlTableSourceList) {
        if(from instanceof SQLExprTableSource ) {
            SQLExprTableSource table = (SQLExprTableSource) from;
            if( table.getExpr() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr)table.getExpr();
                if(sqlIdentifierExpr.getName().toUpperCase().equals(subTableName)) {
                    subSqlTableSourceList.add(table);
                }
            }

        } else if(from instanceof SQLJoinTableSource ){
            SQLJoinTableSource table = ((SQLJoinTableSource)from);
            getSubTableSourceList(table.getLeft(), subTableName, subSqlTableSourceList);
            getSubTableSourceList(table.getRight(), subTableName, subSqlTableSourceList);
        } else if(from instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource table = ((SQLSubqueryTableSource)from);
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)table.getSelect().getQuery();
            getSubTableSourceList(mysqlSelectQuery.getFrom(), subTableName, subSqlTableSourceList);
        }
    }
}