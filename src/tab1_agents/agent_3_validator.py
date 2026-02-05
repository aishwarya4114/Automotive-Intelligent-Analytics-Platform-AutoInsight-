import anthropic
import json
import re
from pathlib import Path
import sys
from typing import Dict, List, Optional

# Add paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))
sys.path.insert(0, str(project_root / 'rag'))
sys.path.insert(0, str(project_root / 'tools'))

from utils.config import Config
from schema_tools import SnowflakeSchemaTools


class ValidatorAgent:
    """
    Agent 3: SQL Validator (FINAL SAFETY CHECK)
    
    Role: Final validation and approval before execution
    Model: Claude Haiku (fast, efficient)
    Tools: SQL validation + cost estimation
    Output: Approval decision with cost estimate
    
    Features:
    - Double-check SQL safety
    - Estimate query cost and complexity
    - HITL approval triggers
    - Row count estimation
    - Risk assessment
    """
    
    def __init__(self, user_tier: str = "analyst"):
        print(" Initializing Agent 3: Validator...")
        
        # Initialize Claude with Haiku (fast validation)
        self.client = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
        self.model = Config.CLAUDE_HAIKU  # Haiku for speed
        
        # Initialize schema tools
        self.schema_tools = SnowflakeSchemaTools()
        
        # User tier for HITL logic
        self.user_tier = user_tier
        
        # HITL thresholds by tier
        self.hitl_thresholds = {
            "analyst": {
                "estimated_cost": 5.0,  # dollars
                "estimated_rows": 500000,
                "table_count": 3
            },
            "manager": {
                "estimated_cost": 20.0,
                "estimated_rows": 1000000,
                "table_count": 5
            },
            "executive": {
                "estimated_cost": float('inf'),  # No limits
                "estimated_rows": float('inf'),
                "table_count": float('inf')
            }
        }
        
        print(f"   User Tier: {user_tier}")
        print(" Validator Agent ready!\n")
    
    def validate_and_approve(
        self, 
        sql_result: Dict,
        query_plan: Dict,
        verbose: bool = True
    ) -> Dict:
        """
        Main method: Final validation and approval
        
        Args:
            sql_result: Output from Agent 2 (contains SQL)
            query_plan: Original query plan from Agent 1
            verbose: Print progress
            
        Returns:
            Dict with approval decision:
            {
                "approved": bool,
                "sql": str,
                "estimated_cost": float,
                "estimated_rows": int,
                "risk_level": str,
                "hitl_required": bool,
                "hitl_reason": str,
                "warnings": List[str],
                "recommendation": str
            }
        """
        if verbose:
            print(f" [AGENT 3] Validating SQL for: '{query_plan['user_query']}'")
        
        # Quick pre-checks
        if not sql_result.get('valid', False):
            return {
                "approved": False,
                "error": "SQL failed Agent 2 validation",
                "sql": sql_result.get('sql', ''),
                "hitl_required": True,
                "hitl_reason": "Invalid SQL"
            }
        
        sql_query = sql_result.get('sql', '')
        if not sql_query:
            return {
                "approved": False,
                "error": "No SQL query provided",
                "hitl_required": True,
                "hitl_reason": "Missing SQL"
            }
        
        # Build validation prompt
        system_prompt = self._get_system_prompt()
        
        messages = [
            {
                "role": "user",
                "content": self._format_validation_request(
                    sql_query, 
                    sql_result, 
                    query_plan
                )
            }
        ]
        
        # Call Claude for validation analysis
        if verbose:
            print(f"   Analyzing query safety and cost...")
        
        response = self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            system=system_prompt,
            messages=messages
        )
        
        # Extract validation result
        validation = self._extract_validation(response, sql_result, verbose)
        
        # Calculate HITL requirement
        hitl_decision = self._check_hitl_required(validation, verbose)
        validation.update(hitl_decision)
        
        # Add final recommendation
        validation['recommendation'] = self._generate_recommendation(validation)
        
        if verbose:
            print(f"    Validation complete!")
            print(f"      Approved: {validation['approved']}")
            print(f"      Risk Level: {validation['risk_level']}")
            print(f"      HITL Required: {validation['hitl_required']}")
            if validation['hitl_required']:
                print(f"      HITL Reason: {validation['hitl_reason']}")
            print()
        
        return validation
    
    def _get_system_prompt(self) -> str:
        """System prompt for Validator"""
        return """You are Agent 3: SQL Validator for AutoInsight, an automotive analytics system.

Your role is to perform FINAL SAFETY VALIDATION before SQL execution on Snowflake.

YOUR TASK:
Analyze the provided SQL query and assess:
1. Safety (is it safe to execute?)
2. Cost (estimated Snowflake compute cost)
3. Complexity (how complex is the query?)
4. Risk level (low, medium, high)

VALIDATION CHECKS:
 No destructive operations (DROP, DELETE, TRUNCATE, ALTER)
 No infinite loops or cross-joins without limits
 Reasonable LIMIT clause (queries without LIMIT should be flagged)
 Proper WHERE clauses on large tables
 JOIN conditions are valid
 No SQL injection patterns

COST ESTIMATION:
- Simple query (single table, basic filters): $0.01 - $0.10
- Medium query (joins, aggregations): $0.10 - $1.00
- Complex query (multiple joins, large tables): $1.00 - $5.00
- Very complex (subqueries, CTEs, cross joins): $5.00+

Base estimates on:
- Number of tables involved
- Estimated rows accessed
- Complexity of JOINs and aggregations
- Whether query has proper filters

RISK LEVELS:
- LOW: Simple SELECT with LIMIT, single table, proper filters
- MEDIUM: Multiple table JOIN, aggregations, reasonable filters
- HIGH: Complex subqueries, missing LIMIT, cross joins, very large row counts

RESPOND WITH JSON:
{
  "safe": true/false,
  "estimated_cost": 0.50,
  "estimated_rows": 10000,
  "complexity": "low|medium|high",
  "risk_level": "low|medium|high",
  "concerns": ["List of any concerns"],
  "warnings": ["List of warnings"],
  "notes": "Any additional notes"
}

BE CONSERVATIVE. If unsure, mark as higher risk and flag for review."""

    def _format_validation_request(
        self, 
        sql_query: str, 
        sql_result: Dict,
        query_plan: Dict
    ) -> str:
        """Format validation request for Claude"""
        
        request = f"""FINAL VALIDATION REQUEST

User Query: "{query_plan['user_query']}"
User Tier: {self.user_tier}

SQL to Validate:
```sql
{sql_query}
```

Context:
- Tables Used: {', '.join(sql_result.get('tables_used', []))}
- Requires JOIN: {sql_result.get('requires_join', False)}
- Query Intent: {query_plan.get('intent', 'Unknown')}
- Analysis Type: {query_plan.get('analysis_type', 'unknown')}

Table Row Counts (approximate):
- FACT_COMPLAINTS: 210,517 rows
- FACT_RECALLS: 2,146 rows
- FACT_RATINGS: 3,090 rows
- FACT_INVESTIGATIONS: 178 rows
- FACT_FUEL_ECONOMY: 8,526 rows
- FACT_ECONOMIC_INDICATORS: 131 rows

Your Task:
1. Validate this SQL is safe to execute
2. Estimate query cost in dollars
3. Estimate rows that will be accessed
4. Assess risk level
5. Identify any concerns or warnings
6. Respond with JSON format specified in system prompt

Analyze now!"""
        
        return request
    
    def _extract_validation(
        self, 
        response, 
        sql_result: Dict,
        verbose: bool
    ) -> Dict:
        """Extract validation result from Claude's response"""
        
        # Get text from response
        text_content = ""
        for block in response.content:
            if hasattr(block, 'text'):
                text_content += block.text
        
        # Try to extract JSON
        try:
            import re
            json_match = re.search(r'\{[\s\S]*\}', text_content)
            
            if json_match:
                validation_json = json.loads(json_match.group())
                
                # Build result
                result = {
                    "approved": validation_json.get('safe', False),
                    "sql": sql_result.get('sql', ''),
                    "estimated_cost": float(validation_json.get('estimated_cost', 0.0)),
                    "estimated_rows": int(validation_json.get('estimated_rows', 0)),
                    "complexity": validation_json.get('complexity', 'unknown'),
                    "risk_level": validation_json.get('risk_level', 'high'),
                    "concerns": validation_json.get('concerns', []),
                    "warnings": validation_json.get('warnings', []),
                    "notes": validation_json.get('notes', ''),
                    "tables_used": sql_result.get('tables_used', []),
                    "requires_join": sql_result.get('requires_join', False),
                    "raw_validation": text_content
                }
                
                return result
        except json.JSONDecodeError:
            if verbose:
                print(f"   ⚠️ Could not parse validation JSON")
        
        # Fallback - manual parsing
        return {
            "approved": False,
            "sql": sql_result.get('sql', ''),
            "estimated_cost": 0.0,
            "estimated_rows": 0,
            "complexity": "unknown",
            "risk_level": "high",
            "concerns": ["Could not parse validation response"],
            "warnings": [],
            "notes": text_content,
            "tables_used": sql_result.get('tables_used', []),
            "requires_join": sql_result.get('requires_join', False)
        }
    
    def _check_hitl_required(self, validation: Dict, verbose: bool) -> Dict:
        """Determine if HITL approval is required"""
        
        # EXECUTIVES NEVER NEED APPROVAL - they are the final approvers
        if self.user_tier == 'executive':
            return {
                "hitl_required": False,
                "hitl_reason": None
            }
        
        # For analyst and manager, check thresholds
        thresholds = self.hitl_thresholds.get(
            self.user_tier, 
            self.hitl_thresholds['analyst']
        )
        
        hitl_reasons = []
        
        # Check cost threshold
        if validation['estimated_cost'] > thresholds['estimated_cost']:
            hitl_reasons.append(
                f"Estimated cost ${validation['estimated_cost']:.2f} exceeds "
                f"${thresholds['estimated_cost']:.2f} limit for {self.user_tier}"
            )
        
        # Check row count threshold
        if validation['estimated_rows'] > thresholds['estimated_rows']:
            hitl_reasons.append(
                f"Estimated {validation['estimated_rows']:,} rows exceeds "
                f"{thresholds['estimated_rows']:,} limit for {self.user_tier}"
            )
        
        # Check table count
        table_count = len(validation.get('tables_used', []))
        if table_count > thresholds['table_count']:
            hitl_reasons.append(
                f"Query uses {table_count} tables, exceeds "
                f"{thresholds['table_count']} limit for {self.user_tier}"
            )
        
        # Check risk level (only for analyst/manager, not executive)
        if validation['risk_level'] == 'high':
            hitl_reasons.append("Query marked as HIGH RISK - requires senior approval")
        
        # Check if unsafe
        if not validation['approved']:
            hitl_reasons.append("Query failed safety validation")
        
        # Check concerns (only severe ones trigger HITL)
        if validation.get('concerns'):
            # Filter for severe concerns only
            severe_concerns = [c for c in validation['concerns'] 
                             if 'DROP' in c or 'DELETE' in c or 'TRUNCATE' in c]
            if severe_concerns:
                hitl_reasons.append(f"Severe safety concerns: {', '.join(severe_concerns)}")
        
        hitl_required = len(hitl_reasons) > 0
        
        return {
            "hitl_required": hitl_required,
            "hitl_reason": "; ".join(hitl_reasons) if hitl_required else None
        }
    
    def _generate_recommendation(self, validation: Dict) -> str:
        """Generate recommendation based on validation"""
        
        if not validation['approved']:
            return " REJECT - SQL failed safety validation"
        
        if validation['hitl_required']:
            return f"⚠️ HITL APPROVAL REQUIRED - {validation['hitl_reason']}"
        
        if validation['risk_level'] == 'low' and validation['estimated_cost'] < 0.10:
            return " APPROVE - Low risk, minimal cost"
        
        if validation['risk_level'] == 'medium' and validation['estimated_cost'] < 1.00:
            return " APPROVE - Medium complexity, acceptable cost"
        
        if validation['warnings']:
            return f"⚠️ APPROVE WITH CAUTION - {len(validation['warnings'])} warning(s)"
        
        return " APPROVE - Query validated"
    
    def close(self):
        """Cleanup resources"""
        self.schema_tools.close()


# ============================================================
# TEST AGENT 3
# ============================================================

def test_agent_3():
    """Test Validator with SQL from Agent 2"""
    
    print("="*80)
    print(" Testing Agent 3: Validator")
    print("="*80)
    print()
    
    # Import Agent 1 and 2
    sys.path.insert(0, str(Path(__file__).parent))
    from agent_1_query_planner import QueryPlannerAgent
    from agent_2_sql_generator import SQLGeneratorAgent
    
    # Initialize all agents
    agent1 = QueryPlannerAgent()
    agent2 = SQLGeneratorAgent(user_tier="analyst")
    agent3 = ValidatorAgent(user_tier="analyst")
    
    # Test queries with different risk levels
    test_queries = [
        "How many recalls does Ford have?",  # Low risk
        "Which manufacturers have the worst customer sentiment?",  # Medium risk
        "Show me all complaints with their full details",  # High risk (no LIMIT implied)
        "Compare Tesla vs Honda sentiment"  # Medium risk
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}: {query}")
        print("="*80)
        
        try:
            # Step 1: Query plan
            print("\n[AGENT 1] Creating query plan...")
            query_plan = agent1.plan_query(query, verbose=False)
            
            # Step 2: Generate SQL
            print("\n[AGENT 2] Generating SQL...")
            sql_result = agent2.generate_sql(query_plan, verbose=False)
            
            if sql_result['valid']:
                print(f"    Valid SQL generated")
                print(f"   Tables: {sql_result.get('tables_used', [])}")
            else:
                print(f"    SQL generation failed")
                continue
            
            # Step 3: Validate and approve
            print("\n[AGENT 3] Validating and approving...")
            approval = agent3.validate_and_approve(
                sql_result, 
                query_plan, 
                verbose=True
            )
            
            # Display result
            print(" VALIDATION RESULT:")
            print("="*80)
            print(f"Decision: {approval['recommendation']}")
            print(f"Estimated Cost: ${approval['estimated_cost']:.2f}")
            print(f"Estimated Rows: {approval['estimated_rows']:,}")
            print(f"Risk Level: {approval['risk_level'].upper()}")
            print(f"Complexity: {approval['complexity']}")
            
            if approval['warnings']:
                print(f"\n Warnings:")
                for warning in approval['warnings']:
                    print(f"   - {warning}")
            
            if approval['concerns']:
                print(f"\n Concerns:")
                for concern in approval['concerns']:
                    print(f"   - {concern}")
            
            print("\nSQL:")
            print("-"*80)
            for line in approval['sql'].split('\n')[:10]:
                print(line)
            if len(approval['sql'].split('\n')) > 10:
                print("...")
            print("-"*80)
            
        except Exception as e:
            print(f"\n Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*80)
    print(" Agent 3 Testing Complete!")
    print("="*80)
    
    # Show HITL thresholds
    print(f"\n HITL Thresholds for 'analyst' tier:")
    print(f"   Max Cost: ${agent3.hitl_thresholds['analyst']['estimated_cost']}")
    print(f"   Max Rows: {agent3.hitl_thresholds['analyst']['estimated_rows']:,}")
    print(f"   Max Tables: {agent3.hitl_thresholds['analyst']['table_count']}")
    
    agent1.close()
    agent2.close()
    agent3.close()


def demo_full_pipeline():
    """Demo complete Agent 1 → Agent 2 → Agent 3 pipeline"""
    
    print("="*80)
    print(" Full Pipeline Demo: Agent 1 → Agent 2 → Agent 3")
    print("="*80)
    print()
    print("Ask questions and see the complete validation pipeline!")
    print("Type 'exit' to quit")
    print("="*80)
    print()
    
    # Import agents
    sys.path.insert(0, str(Path(__file__).parent))
    from agent_1_query_planner import QueryPlannerAgent
    from agent_2_sql_generator import SQLGeneratorAgent
    
    agent1 = QueryPlannerAgent()
    agent2 = SQLGeneratorAgent(user_tier="analyst")
    agent3 = ValidatorAgent(user_tier="analyst")
    
    while True:
        try:
            user_query = input("\n Your question: ").strip()
            
            if user_query.lower() in ['exit', 'quit', 'q']:
                print("\n Goodbye!")
                break
            
            if not user_query:
                continue
            
            print("\n" + "="*80)
            
            # Agent 1: Plan
            print("[AGENT 1] Planning query...")
            query_plan = agent1.plan_query(user_query, verbose=False)
            print(f"   Intent: {query_plan.get('intent', 'N/A')}")
            
            # Agent 2: Generate SQL
            print("\n[AGENT 2] Generating SQL...")
            sql_result = agent2.generate_sql(query_plan, verbose=False)
            
            if not sql_result['valid']:
                print(f"    SQL generation failed")
                continue
            
            print(f"    SQL generated")
            
            # Agent 3: Validate
            print("\n[AGENT 3] Validating...")
            approval = agent3.validate_and_approve(sql_result, query_plan, verbose=False)
            
            # Display
            print("\n" + "="*80)
            print(f" {approval['recommendation']}")
            print("="*80)
            print(f"Cost: ${approval['estimated_cost']:.2f} | "
                  f"Rows: {approval['estimated_rows']:,} | "
                  f"Risk: {approval['risk_level'].upper()}")
            
            if approval['hitl_required']:
                print(f"\n HITL APPROVAL REQUIRED:")
                print(f"   {approval['hitl_reason']}")
            else:
                print(f"\n APPROVED FOR EXECUTION")
            
            print(f"\nGenerated SQL:")
            print("-"*80)
            print(approval['sql'])
            print("-"*80)
            
        except KeyboardInterrupt:
            print("\n\n Goodbye!")
            break
        except Exception as e:
            print(f"\n Error: {e}")
    
    agent1.close()
    agent2.close()
    agent3.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demo_full_pipeline()
    else:
        test_agent_3()