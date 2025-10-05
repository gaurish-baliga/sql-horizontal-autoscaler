package parser

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// ParseResult contains the result of parsing a SQL query
type ParseResult struct {
	TableName    string
	ShardKeyValue interface{}
	HasShardKey  bool
}

// Parse parses a SQL query and extracts the shard key value if present
func Parse(query string, tableShardKeys map[string]string) (*ParseResult, error) {
	// Parse the SQL query
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	result := &ParseResult{}

	// Handle different types of SQL statements
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return parseSelect(stmt, tableShardKeys)
	case *sqlparser.Insert:
		return parseInsert(stmt, tableShardKeys)
	case *sqlparser.Update:
		return parseUpdate(stmt, tableShardKeys)
	case *sqlparser.Delete:
		return parseDelete(stmt, tableShardKeys)
	default:
		return result, fmt.Errorf("unsupported SQL statement type")
	}
}

// parseSelect handles SELECT statements
func parseSelect(stmt *sqlparser.Select, tableShardKeys map[string]string) (*ParseResult, error) {
	result := &ParseResult{}

	// Extract table name from FROM clause
	if len(stmt.From) == 0 {
		return result, fmt.Errorf("no FROM clause found")
	}

	tableName := extractTableName(stmt.From[0])
	if tableName == "" {
		return result, fmt.Errorf("could not extract table name")
	}

	result.TableName = tableName

	// Check if this table has a shard key configured
	shardKey, exists := tableShardKeys[tableName]
	if !exists {
		return result, nil // No shard key configured for this table
	}

	// Extract shard key value from WHERE clause
	if stmt.Where != nil {
		shardKeyValue := extractShardKeyValue(stmt.Where.Expr, shardKey)
		if shardKeyValue != nil {
			result.ShardKeyValue = shardKeyValue
			result.HasShardKey = true
		}
	}

	return result, nil
}

// parseInsert handles INSERT statements
func parseInsert(stmt *sqlparser.Insert, tableShardKeys map[string]string) (*ParseResult, error) {
	result := &ParseResult{}

	tableName := stmt.Table.Name.String()
	result.TableName = tableName

	// Check if this table has a shard key configured
	shardKey, exists := tableShardKeys[tableName]
	if !exists {
		return result, nil
	}

	// For INSERT statements, we need to find the shard key in the column list
	if rows, ok := stmt.Rows.(sqlparser.Values); ok && len(rows) > 0 {
		// Find the column index for the shard key
		for i, col := range stmt.Columns {
			if col.String() == shardKey {
				// Extract the value from the first row
				if i < len(rows[0]) {
					if val := extractLiteralValue(rows[0][i]); val != nil {
						result.ShardKeyValue = val
						result.HasShardKey = true
					}
				}
				break
			}
		}
	}

	return result, nil
}

// parseUpdate handles UPDATE statements
func parseUpdate(stmt *sqlparser.Update, tableShardKeys map[string]string) (*ParseResult, error) {
	result := &ParseResult{}

	tableName := extractTableName(stmt.TableExprs[0])
	if tableName == "" {
		return result, fmt.Errorf("could not extract table name from UPDATE")
	}
	result.TableName = tableName

	// Check if this table has a shard key configured
	shardKey, exists := tableShardKeys[tableName]
	if !exists {
		return result, nil
	}

	// Extract shard key value from WHERE clause
	if stmt.Where != nil {
		shardKeyValue := extractShardKeyValue(stmt.Where.Expr, shardKey)
		if shardKeyValue != nil {
			result.ShardKeyValue = shardKeyValue
			result.HasShardKey = true
		}
	}

	return result, nil
}

// parseDelete handles DELETE statements
func parseDelete(stmt *sqlparser.Delete, tableShardKeys map[string]string) (*ParseResult, error) {
	result := &ParseResult{}

	tableName := extractTableName(stmt.TableExprs[0])
	if tableName == "" {
		return result, fmt.Errorf("could not extract table name from DELETE")
	}
	result.TableName = tableName

	// Check if this table has a shard key configured
	shardKey, exists := tableShardKeys[tableName]
	if !exists {
		return result, nil
	}

	// Extract shard key value from WHERE clause
	if stmt.Where != nil {
		shardKeyValue := extractShardKeyValue(stmt.Where.Expr, shardKey)
		if shardKeyValue != nil {
			result.ShardKeyValue = shardKeyValue
			result.HasShardKey = true
		}
	}

	return result, nil
}

// extractTableName extracts the table name from a TableExpr
func extractTableName(tableExpr sqlparser.TableExpr) string {
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := table.Expr.(sqlparser.TableName); ok {
			return tableName.Name.String()
		}
	}
	return ""
}

// extractShardKeyValue recursively searches for the shard key in the WHERE expression
func extractShardKeyValue(expr sqlparser.Expr, shardKey string) interface{} {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Check if this is a comparison with our shard key
		if colName, ok := expr.Left.(*sqlparser.ColName); ok {
			if colName.Name.String() == shardKey && expr.Operator == "=" {
				return extractLiteralValue(expr.Right)
			}
		}
	case *sqlparser.AndExpr:
		// Recursively check both sides of AND
		if val := extractShardKeyValue(expr.Left, shardKey); val != nil {
			return val
		}
		return extractShardKeyValue(expr.Right, shardKey)
	case *sqlparser.OrExpr:
		// For OR expressions, we can't determine a single shard
		return nil
	}
	return nil
}

// extractLiteralValue extracts the actual value from a literal expression
func extractLiteralValue(expr sqlparser.Expr) interface{} {
	switch val := expr.(type) {
	case *sqlparser.SQLVal:
		switch val.Type {
		case sqlparser.StrVal:
			return string(val.Val)
		case sqlparser.IntVal:
			return string(val.Val)
		case sqlparser.FloatVal:
			return string(val.Val)
		}
	}
	return nil
}
