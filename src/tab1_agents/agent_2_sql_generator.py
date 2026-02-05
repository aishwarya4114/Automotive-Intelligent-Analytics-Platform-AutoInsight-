"""
Agent 2: SQL Generator - ROBUST VERSION for Complex Queries

Key improvements:
- Better handling of multi-table JOINs
- Prevents infinite tool-calling loops
- Forces SQL generation after reasonable schema retrieval
- Better error messages and debugging
"""

import anthropic
import json
from pathlib import Path
import sys
from typing import Dict, List
import re

# Add paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))
sys.path.insert(0, str(project_root / 'src' / 'auth'))
sys.path.insert(0, str(project_root / 'tools'))
sys.path.insert(0, str(project_root / 'src' / 'rag'))
sys.path.insert(0, str(project_root / 'src' / 'tools'))
sys.path.insert(0, str(project_root / 'src' / 'data_cleaning'))

from data_cleaning.utils.config import Config
from tools.schema_tools import SnowflakeSchemaTools
from auth.user_management import RBACRules


class SQLGeneratorWithRBAC:
    """Agent 2: Robust SQL Generator with RBAC"""
    
    def __init__(self, user_tier: str = "analyst"):
        self.client = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
        self.model = Config.CLAUDE_SONNET
        self.schema_tools = SnowflakeSchemaTools()
        self.user_tier = user_tier
    
    def generate_sql(self, query_plan: Dict, verbose: bool = True) -> Dict:
        """Generate SQL with better complex query handling"""
        
        # Check access
        access_check = self._check_query_access(query_plan)
        
        if not access_check['allowed']:
            return {
                "sql": None,
                "valid": False,
                "access_denied": True,
                "error": access_check['error_message'],
                "blocked_columns": access_check['blocked_columns'],
                "user_tier": self.user_tier
            }
        
        # Build system prompt
        system_prompt = self._get_system_prompt_robust()
        
        # Build initial message
        messages = [{
            "role": "user",
            "content": self._format_query_plan_robust(query_plan)
        }]
        
        # Modified loop with better control
        max_iterations = 10  # Increased
        iteration = 0
        schema_calls = 0
        max_schema_calls = 3  # Limit schema searches
        last_sql = None
        
        while iteration < max_iterations:
            iteration += 1
            
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=self._get_tool_definitions(),
                    messages=messages
                )
            except Exception as e:
                return {
                    "sql": None,
                    "valid": False,
                    "error": f"API Error: {str(e)}"
                }
            
            # Check stop reason
            if response.stop_reason == "tool_use":
                # Count schema calls
                for block in response.content:
                    if hasattr(block, 'type') and block.type == "tool_use":
                        if block.name == "get_snowflake_schema":
                            schema_calls += 1
                
                # Process tool calls FIRST (required by API)
                tool_results = []
                for block in response.content:
                    if hasattr(block, 'type') and block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input
                        
                        if tool_name == "get_snowflake_schema":
                            result = self.schema_tools.get_snowflake_schema(
                                keyword=tool_input['keyword'],
                                top_k=tool_input.get('top_k', 2)
                            )
                        elif tool_name == "validate_sql_against_schema":
                            result = self.schema_tools.validate_sql_against_schema(
                                sql_query=tool_input['sql_query']
                            )
                            result = json.dumps(result, indent=2)
                        else:
                            result = {"error": f"Unknown tool: {tool_name}"}
                        
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result
                        })
                
                # Add assistant message and tool results
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})
                
                # Force SQL generation if too many schema calls
                if schema_calls >= max_schema_calls:
                    force_prompt = f"""You have now retrieved schema for {schema_calls} tables.

You have ALL the schema information needed. Do NOT call get_snowflake_schema again.

User question: "{query_plan['user_query']}"

Generate the complete SQL query NOW using the tables you found:
- Use AUTOMOTIVE_AI.NHTSA.FACT_RECALLS for recalls
- Use AUTOMOTIVE_AI.NHTSA.FACT_RATINGS for safety ratings
- Use AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS for sentiment scores

Create a SQL query with LEFT JOINs on make (manufacturer). Include WHERE, GROUP BY, ORDER BY, and LIMIT 20.

Respond with SQL in ```sql code block NOW. Do not call any more tools."""
                    
                    messages.append({"role": "user", "content": force_prompt})
                
                continue
                
                # Process tools normally
                tool_results = []
                for block in response.content:
                    if hasattr(block, 'type') and block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input
                        
                        if tool_name == "get_snowflake_schema":
                            result = self.schema_tools.get_snowflake_schema(
                                keyword=tool_input['keyword'],
                                top_k=tool_input.get('top_k', 3)
                            )
                        elif tool_name == "validate_sql_against_schema":
                            result = self.schema_tools.validate_sql_against_schema(
                                sql_query=tool_input['sql_query']
                            )
                            result = json.dumps(result, indent=2)
                        else:
                            result = {"error": f"Unknown tool: {tool_name}"}
                        
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result
                        })
                
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})
            
            else:
                # Agent stopped - should have SQL
                sql = self._extract_sql_from_response(response)
                last_sql = sql
                
                if sql:
                    # Validate it
                    validation = self.schema_tools.validate_sql_against_schema(sql)
                    
                    if validation.get('valid'):
                        return {
                            "sql": sql,
                            "valid": True,
                            "tables_used": validation.get('tables_used', []),
                            "user_tier": self.user_tier
                        }
                    else:
                        # SQL invalid - provide feedback and retry
                        if iteration < max_iterations - 1:
                            error_msg = "SQL validation failed: " + "; ".join(validation.get('errors', []))
                            messages.append({"role": "assistant", "content": response.content})
                            messages.append({"role": "user", "content": error_msg + "\n\nGenerate corrected SQL."})
                        else:
                            # Last iteration
                            return {
                                "sql": sql,
                                "valid": False,
                                "error": "SQL validation failed",
                                "validation_errors": validation.get('errors', [])
                            }
                else:
                    # No SQL extracted - try to get Claude to generate it
                    if iteration < max_iterations - 1:
                        hint_prompt = f"""I need you to generate SQL for: "{query_plan['user_query']}"

The query needs:
- FACT_RECALLS for recall counts
- FACT_RATINGS for safety ratings  
- FACT_COMPLAINTS for sentiment scores

Generate this SQL:

```sql
SELECT 
    r.make,
    COUNT(DISTINCT r.recall_id) as recall_count,
    AVG(rt.overall_stars) as avg_safety_rating,
    AVG(c.sentiment_score) as avg_sentiment
FROM AUTOMOTIVE_AI.NHTSA.FACT_RECALLS r
LEFT JOIN AUTOMOTIVE_AI.NHTSA.FACT_RATINGS rt ON UPPER(r.make) = UPPER(rt.make)
LEFT JOIN AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS c ON UPPER(r.make) = UPPER(c.make)
WHERE r.year BETWEEN 2015 AND 2025
GROUP BY r.make
HAVING COUNT(DISTINCT r.recall_id) > 0
ORDER BY recall_count DESC
LIMIT 20;
```

Provide similar SQL in a code block."""
                        
                        messages.append({"role": "assistant", "content": response.content})
                        messages.append({"role": "user", "content": hint_prompt})
                    else:
                        return {
                            "sql": None,
                            "valid": False,
                            "error": "Could not extract SQL from response"
                        }
        
        # Max iterations reached
        return {
            "sql": last_sql,
            "valid": False,
            "error": f"Max iterations ({max_iterations}) reached. Last SQL: {'Found' if last_sql else 'None'}",
            "debug_last_sql": last_sql,
            "debug_iterations": iteration
        }
    
    def _extract_sql_from_response(self, response) -> str:
        """Extract SQL from Claude's response"""
        
        text = ""
        for block in response.content:
            if hasattr(block, 'text'):
                text += block.text
        
        # Try ```sql first
        match = re.search(r'```sql\n(.*?)\n```', text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Try generic ```
        match = re.search(r'```\n(.*?)\n```', text, re.DOTALL)
        if match:
            sql = match.group(1).strip()
            if 'SELECT' in sql.upper():
                return sql
        
        # Try finding SELECT statement
        lines = []
        in_sql = False
        for line in text.split('\n'):
            if 'SELECT' in line.upper():
                in_sql = True
            if in_sql:
                lines.append(line)
                if ';' in line:
                    break
        
        if lines:
            return '\n'.join(lines).strip()
        
        return None
    
    def _get_system_prompt_robust(self) -> str:
        """Enhanced system prompt for complex queries"""
        
        return f"""You are Agent 2: SQL Generator for AutoInsight.

USER TIER: {self.user_tier.upper()}

CRITICAL WORKFLOW FOR MULTI-TABLE QUERIES:
1. Call get_snowflake_schema for each needed table (MAX 3 calls total)
2. Review the schema information returned
3. STOP calling tools
4. Generate complete SQL with all JOINs
5. Call validate_sql_against_schema ONCE
6. If valid, stop. If invalid, fix and regenerate.

DO NOT call get_snowflake_schema more than 3 times. After 3 schema searches, you have enough information.

AVAILABLE TABLES (use these EXACT names):
- AUTOMOTIVE_AI.NHTSA.FACT_RECALLS
- AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS  
- AUTOMOTIVE_AI.NHTSA.FACT_RATINGS
- AUTOMOTIVE_AI.NHTSA.FACT_INVESTIGATIONS
- AUTOMOTIVE_AI.EPA.FACT_FUEL_ECONOMY
- AUTOMOTIVE_AI.FRED.FACT_ECONOMIC_INDICATORS

MULTI-TABLE JOIN PATTERN:

For: "Manufacturers with recalls, ratings, and sentiment"

```sql
SELECT 
    r.make,
    COUNT(DISTINCT r.recall_id) as recall_count,
    AVG(rt.overall_stars) as avg_rating,
    AVG(c.sentiment_score) as avg_sentiment,
    COUNT(DISTINCT c.complaint_id) as complaint_count
FROM AUTOMOTIVE_AI.NHTSA.FACT_RECALLS r
LEFT JOIN AUTOMOTIVE_AI.NHTSA.FACT_RATINGS rt 
    ON UPPER(r.make) = UPPER(rt.make)
LEFT JOIN AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS c 
    ON UPPER(r.make) = UPPER(c.make)
WHERE r.year BETWEEN 2015 AND 2025
GROUP BY r.make
HAVING COUNT(DISTINCT r.recall_id) > 0
ORDER BY recall_count DESC
LIMIT 20;
```

KEY POINTS:
- Use LEFT JOIN to preserve primary table rows
- Use UPPER() for case-insensitive matching
- Join on make (manufacturer name)
- Can also join on make + year for more precision
- Use COUNT(DISTINCT) to avoid duplicates
- Always include LIMIT

RESPOND WITH SQL after retrieving schema. Do not get stuck in endless tool calling."""
    
    def _format_query_plan_robust(self, query_plan: Dict) -> str:
        """Format with clear instructions"""
        
        return f"""USER QUERY: "{query_plan['user_query']}"

User Tier: {self.user_tier}
Intent: {query_plan.get('intent', 'Unknown')}

INSTRUCTIONS:
1. Identify which tables you need (recalls, ratings, complaints, fuel economy, etc.)
2. Call get_snowflake_schema for each table (maximum 3 calls)
3. After getting schema, immediately generate SQL with JOINs if needed
4. Validate the SQL once
5. Return the validated SQL

START NOW. Call get_snowflake_schema for the first table you need."""
    
    def _check_query_access(self, query_plan: Dict) -> Dict:
        """Check access (same as before)"""
        query_lower = query_plan['user_query'].lower()
        
        if self.user_tier in ['analyst', 'manager']:
            if any(word in query_lower for word in ['vin', 'vehicle identification']):
                return {
                    "allowed": False,
                    "error_message": (
                        f"Access Denied. You do not have sufficient permissions to access VIN data. "
                        f"Your role ({self.user_tier}) does not permit access to personally identifiable information."
                    ),
                    "blocked_columns": ["VIN"],
                    "required_tier": "executive"
                }
        
        if self.user_tier == 'analyst':
            detail_keywords = [
                'complaint description', 'complaint text',
                'component description', 'recall summary'
            ]
            
            if any(keyword in query_lower for keyword in detail_keywords):
                return {
                    "allowed": False,
                    "error_message": (
                        f"Access Denied. Your role (analyst) does not have access to detailed text fields. "
                        f"Manager or executive role required."
                    ),
                    "blocked_columns": ["complaint_description", "component_description"],
                    "required_tier": "manager or executive"
                }
        
        return {"allowed": True, "error_message": None, "blocked_columns": []}
    
    def _get_tool_definitions(self) -> List[Dict]:
        """Tool definitions"""
        return [
            {
                "name": "get_snowflake_schema",
                "description": "Get table schema. Call maximum 3 times total.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "keyword": {"type": "string"},
                        "top_k": {"type": "integer", "default": 2}
                    },
                    "required": ["keyword"]
                }
            },
            {
                "name": "validate_sql_against_schema",
                "description": "Validate generated SQL. Call once after generating SQL.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "sql_query": {"type": "string"}
                    },
                    "required": ["sql_query"]
                }
            }
        ]
    
    def close(self):
        """Cleanup"""
        self.schema_tools.close()