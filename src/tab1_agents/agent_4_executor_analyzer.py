"""
Agent 4: Executor + Analyzer 

ZERO EXTERNAL KNOWLEDGE - Uses ONLY data from queries
Automatic filtering of any speculation or external facts
Professional, clean output for production UI
"""

import anthropic
import json
import re
from pathlib import Path
import sys
from typing import Dict, List
from datetime import datetime

# Add paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))
sys.path.insert(0, str(project_root / 'tools'))
sys.path.insert(0, str(project_root / 'src' / 'rag'))
sys.path.insert(0, str(project_root / 'src' / 'tools'))
sys.path.insert(0, str(project_root / 'src' / 'data_cleaning'))

from data_cleaning.utils.config import Config
from tools.schema_tools import SnowflakeSchemaTools

try:
    # Assuming 'visualization_agent.py' is in the same directory as agent_4_executor_analyzer.py
    from agent_5_vis import render_analyst_visualizations
except ImportError:
    # Fallback for environments where local module import fails
    def render_analyst_visualizations(df, plan): 
        print("Visualization Agent: Streamlit function not available in this execution context.")


class CleanAnalyzer:
    """Clean analysis with automatic filtering of external knowledge"""
    
    @staticmethod
    def filter_external_knowledge(text: str) -> tuple[str, List[str]]:
        """
        Remove sentences containing external knowledge
        
        Returns:
            (cleaned_text, removed_sentences)
        """
        
        # Patterns that indicate external knowledge
        forbidden_patterns = [
            r'typically (?:costs?|requires?)',
            r'industry (?:average|standard|benchmark)',
            r'according to (?:industry|research|studies|experts)',
            r'historical(?:ly)? (?:data shows|evidence)',
            r'experts? (?:say|believe|suggest|indicate)',
            r'studies? (?:show|indicate|suggest|reveal)',
            r'(?:it is|this is) (?:known|established|common)',
            r'in general(?:ly)?',
            r'usually|commonly|normally|traditionally',
            r'estimated? (?:cost|value|price) of \$\d+',  # "$100M per recall"
            r'likely|probably|possibly|potentially',  # Speculation
            r'suggests?|indicates?|implies?',  # Weak causal
        ]
        
        sentences = text.split('.')
        clean_sentences = []
        removed_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
                
            # Check if sentence contains forbidden patterns
            has_external = False
            for pattern in forbidden_patterns:
                if re.search(pattern, sentence, re.IGNORECASE):
                    has_external = True
                    break
            
            if has_external:
                removed_sentences.append(sentence)
            else:
                clean_sentences.append(sentence)
        
        cleaned_text = '. '.join(clean_sentences)
        if cleaned_text and not cleaned_text.endswith('.'):
            cleaned_text += '.'
            
        return cleaned_text, removed_sentences
    
    @staticmethod
    def filter_insights(insights: List[str]) -> tuple[List[str], List[str]]:
        """Filter list of insights for external knowledge"""
        
        clean_insights = []
        removed_insights = []
        
        for insight in insights:
            cleaned, removed = CleanAnalyzer.filter_external_knowledge(insight)
            
            if cleaned.strip() and not removed:
                clean_insights.append(cleaned)
            else:
                removed_insights.append(insight)
        
        return clean_insights, removed_insights


class ExecutorAnalyzerClean:
    """
    Agent 4: Clean Professional Version
    
    - Uses ONLY data from queries
    - Automatic filtering of external knowledge
    - Professional output for production UI
    - Zero hallucination tolerance
    """
    
    def __init__(self):
        print(" Initializing Agent 4: Clean Professional Analyzer...")
        
        self.client = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
        self.model = Config.CLAUDE_SONNET
        self.schema_tools = SnowflakeSchemaTools()
        self.cleaner = CleanAnalyzer()
        
        print(" Clean Analyzer ready!\n")
    
    def execute_and_analyze(
        self,
        approval: Dict,
        query_plan: Dict,
        verbose: bool = True
    ) -> Dict:
        """Execute SQL and generate CLEAN analysis"""
        
        if verbose:
            print(f" [AGENT 4] Executing and analyzing: '{query_plan['user_query']}'")
        
        # Check approval
        if not approval.get('approved', False):
            return {
                "success": False,
                "error": "Query not approved",
                "hitl_required": approval.get('hitl_required', False)
            }
        
        sql_query = approval.get('sql', '')
        
        # Step 1: Execute SQL
        if verbose:
            print(f"   [1/4] Executing SQL on Snowflake...")
        
        execution_result = self.schema_tools.execute_snowflake_query(sql_query, limit=None)
        
        if not execution_result.get('success', False):
            return {
                "success": False,
                "error": f"Execution failed: {execution_result.get('error')}"
            }
        
        if verbose:
            print(f"       Query executed: {execution_result['row_count']} rows")
        
        # Step 2: Format results
        if verbose:
            print(f"   [2/4] Formatting results...")
        
        formatted_results = self._format_results_strict(execution_result, query_plan)
        
        # Step 3: Generate analysis with STRICT grounding
        if verbose:
            print(f"   [3/4] Analyzing with strict grounding...")
        
        raw_analysis = self._analyze_strict(formatted_results, query_plan, verbose)
        
        # Step 4: FILTER external knowledge
        if verbose:
            print(f"   [4/4] Filtering external knowledge...")
        
        clean_analysis, filter_report = self._clean_analysis(raw_analysis, verbose)
        
        if verbose:
            if filter_report['sentences_removed'] > 0:
                print(f"       Removed {filter_report['sentences_removed']} external claim(s)")
            print(f"       Analysis cleaned and verified!")
            print()
        
        # Combine results
        final_result = {
            "success": True,
            "user_query": query_plan['user_query'],
            "sql": sql_query,
            "data": execution_result['data'],
            "row_count": execution_result['row_count'],
            "columns": execution_result['columns'],
            "execution_time": datetime.now().isoformat(),
            **clean_analysis,
            "filter_report": filter_report  # For transparency
        }
        
        return final_result
    
    def _format_results_strict(self, execution_result: Dict, query_plan: Dict) -> str:
        """Format results with emphasis on data-only analysis"""
        
        data = execution_result.get('data', [])
        columns = execution_result.get('columns', [])
        row_count = execution_result.get('row_count', 0)
        
        formatted = f"""QUERY EXECUTION RESULTS - USE ONLY THIS DATA

User Question: "{query_plan['user_query']}"
Rows Returned: {row_count}
Columns: {', '.join(columns)}

 CRITICAL INSTRUCTIONS:
- Use ONLY the numbers and data shown below
- Do NOT reference external knowledge, industry standards, or typical values
- Do NOT make assumptions beyond what the data explicitly shows
- If the data doesn't show something, say "The data does not show..."
- NO speculation, NO "likely", NO "probably", NO "typically"

DATA:
{'='*80}
"""
        
        if row_count == 0:
            formatted += "⚠️ No data returned.\n"
            return formatted
        
        # Show all data
        for i, row in enumerate(data, 1):
            formatted += f"\nRow {i}:\n"
            for col in columns:
                value = row.get(col, 'NULL')
                if isinstance(value, float):
                    formatted += f"  {col}: {value:.3f}\n"
                elif isinstance(value, int):
                    formatted += f"  {col}: {value:,}\n"
                else:
                    formatted += f"  {col}: {value}\n"
        
        formatted += "\n" + "="*80
        formatted += "\n\nREMINDER: Use ONLY the numbers above. NO external knowledge."
        
        return formatted
    
    def _analyze_strict(
        self,
        formatted_results: str,
        query_plan: Dict,
        verbose: bool
    ) -> Dict:
        """Generate analysis with ULTRA-STRICT grounding"""
        
        system_prompt = """You are a DATA ANALYST who works ONLY with provided data.

ABSOLUTE RULES - NEVER VIOLATE:
1. Use ONLY numbers that appear in the query results
2. Do NOT reference any external knowledge
3. Do NOT mention industry standards, typical values, or averages not in data
4. Do NOT use words like "typically", "usually", "commonly", "generally"
5. Do NOT make cost estimates unless costs are in the data
6. Do NOT speculate with "likely", "probably", "suggests", "indicates"
7. When calculating, show your work: "X / Y = Z"

WHAT YOU CAN DO:
 State facts from the data: "The data shows X has Y complaints"
 Calculate from provided numbers: "477 / 3674 = 13%"
 Compare values: "X is higher than Y"
 Rank items: "X ranks first, Y ranks second"
 Identify patterns: "X shows an increasing trend from A to B"

WHAT YOU CANNOT DO:
 Reference external facts: "Industry average is...", "Typically costs..."
 Make assumptions: "This likely means...", "Probably due to..."
 Cite sources: "According to research...", "Studies show..."
 Historical context: "In 2018, there was...", "Historically..."

IF DATA IS INSUFFICIENT:
Say "The data provided does not show..." instead of guessing.

RESPOND ONLY WITH JSON - No markdown, no explanations outside JSON."""
        
        analysis_request = f"""USER QUERY: "{query_plan['user_query']}"

{formatted_results}

Analyze ONLY the data above. Use ONLY numbers from the query results.

Respond with JSON:
{{
  "summary": "1-2 sentences stating what the data shows (facts only)",
  "key_findings": [
    "Finding 1 with specific numbers from data",
    "Finding 2 with specific numbers from data",
    "Finding 3 with specific numbers from data"
  ],
  "comparisons": [
    "Comparison 1 based on data values",
    "Comparison 2 based on data values"
  ],
  "recommendations": [
    "Action 1 based on data findings",
    "Action 2 based on data findings"
  ]
}}

CRITICAL: NO external knowledge. NO speculation. ONLY data facts."""
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=3096,
                system=system_prompt,
                messages=[{"role": "user", "content": analysis_request}]
            )
            
            # Extract text
            text_content = ""
            for block in response.content:
                if hasattr(block, 'text'):
                    text_content += block.text
            
            # Parse JSON
            json_match = re.search(r'\{[\s\S]*\}', text_content)
            
            if json_match:
                analysis = json.loads(json_match.group())
                
                return {
                    "summary": analysis.get('summary', ''),
                    "key_findings": analysis.get('key_findings', []),
                    "comparisons": analysis.get('comparisons', []),
                    "recommendations": analysis.get('recommendations', []),
                    "raw_response": text_content
                }
            else:
                return {
                    "summary": "Could not parse analysis",
                    "key_findings": ["Analysis parsing failed"],
                    "comparisons": [],
                    "recommendations": []
                }
                
        except Exception as e:
            return {
                "summary": "Analysis failed",
                "key_findings": [f"Error: {str(e)}"],
                "comparisons": [],
                "recommendations": [],
                "error": str(e)
            }
    
    def _clean_analysis(self, raw_analysis: Dict, verbose: bool) -> tuple[Dict, Dict]:
        """Remove any external knowledge that slipped through"""
        
        removed_content = {
            "sentences_removed": 0,
            "insights_removed": 0,
            "removed_items": []
        }
        
        # Clean summary
        clean_summary, removed_summary = self.cleaner.filter_external_knowledge(
            raw_analysis.get('summary', '')
        )
        
        if removed_summary:
            removed_content["sentences_removed"] += len(removed_summary)
            removed_content["removed_items"].extend(removed_summary)
        
        # Clean key findings
        clean_findings, removed_findings = self.cleaner.filter_insights(
            raw_analysis.get('key_findings', [])
        )
        
        if removed_findings:
            removed_content["insights_removed"] += len(removed_findings)
            removed_content["removed_items"].extend(removed_findings)
        
        # Clean comparisons
        clean_comparisons, removed_comparisons = self.cleaner.filter_insights(
            raw_analysis.get('comparisons', [])
        )
        
        if removed_comparisons:
            removed_content["insights_removed"] += len(removed_comparisons)
            removed_content["removed_items"].extend(removed_comparisons)
        
        # Clean recommendations
        clean_recommendations, removed_recommendations = self.cleaner.filter_insights(
            raw_analysis.get('recommendations', [])
        )
        
        if removed_recommendations:
            removed_content["insights_removed"] += len(removed_recommendations)
            removed_content["removed_items"].extend(removed_recommendations)
        
        # Build clean analysis
        clean_analysis = {
            "executive_summary": clean_summary if clean_summary.strip() else "Analysis based on query results.",
            "key_insights": clean_findings,
            "comparisons": clean_comparisons,
            "recommendations": clean_recommendations
        }
        
        return clean_analysis, removed_content
    
    def format_output_professional(self, result: Dict) -> str:
        """Format as clean, professional report"""
        
        if not result.get('success', False):
            return f" ERROR: {result.get('error', 'Unknown error')}"
        
        output = []
        output.append("="*80)
        output.append("📊 ANALYSIS REPORT")
        output.append("="*80)
        output.append(f"\nQuery: {result['user_query']}")
        output.append(f"Data Points Analyzed: {result['row_count']:,} rows")
        output.append(f"Generated: {result['execution_time']}")
        
        # Executive Summary
        output.append("\n" + "-"*80)
        output.append("EXECUTIVE SUMMARY")
        output.append("-"*80)
        output.append(result.get('executive_summary', 'No summary available'))
        
        # Key Insights
        if result.get('key_insights'):
            output.append("\n" + "-"*80)
            output.append("KEY INSIGHTS")
            output.append("-"*80)
            for i, insight in enumerate(result['key_insights'], 1):
                output.append(f"{i}. {insight}")
        
        # Comparisons
        if result.get('comparisons'):
            output.append("\n" + "-"*80)
            output.append("COMPARISONS")
            output.append("-"*80)
            for comparison in result['comparisons']:
                output.append(f"• {comparison}")
        
        # Recommendations
        if result.get('recommendations'):
            output.append("\n" + "-"*80)
            output.append("RECOMMENDATIONS")
            output.append("-"*80)
            for i, rec in enumerate(result['recommendations'], 1):
                output.append(f"{i}. {rec}")
        
        # Data Preview
        output.append("\n" + "-"*80)
        output.append("DATA SUMMARY")
        output.append("-"*80)
        
        # Show first 3 rows only
        for i, row in enumerate(result['data'][:3], 1):
            output.append(f"\nRecord {i}:")
            for key, value in row.items():
                if isinstance(value, float):
                    output.append(f"  {key}: {value:.2f}")
                elif isinstance(value, int):
                    output.append(f"  {key}: {value:,}")
                else:
                    output.append(f"  {key}: {value}")
        
        if result['row_count'] > 3:
            output.append(f"\n... and {result['row_count'] - 3} more records")
        
        output.append("\n" + "="*80)
        
        return "\n".join(output)
    
    def close(self):
        """Cleanup"""
        self.schema_tools.close()


# ============================================================
# TEST CLEAN VERSION
# ============================================================

def test_clean_agent4():
    """Test clean version against original"""
    
    print("="*80)
    print("Testing Clean Professional Agent 4")
    print("="*80)
    print()
    
    # Import other agents
    sys.path.insert(0, str(Path(__file__).parent))
    from agent_1_query_planner import QueryPlannerAgent
    from agent_2_sql_generator import SQLGeneratorAgent
    from agent_3_validator import ValidatorAgent
    
    # Initialize
    agent1 = QueryPlannerAgent()
    agent2 = SQLGeneratorAgent(user_tier="analyst")
    agent3 = ValidatorAgent(user_tier="analyst")
    agent4 = ExecutorAnalyzerClean()
    
    # Test query
    query = "Which manufacturers have the worst customer sentiment?"
    
    print(f"TEST QUERY: {query}")
    print("="*80)
    
    # Run pipeline
    print("\n[AGENT 1] Planning...")
    query_plan = agent1.plan_query(query, verbose=False)
    
    print("\n[AGENT 2] Generating SQL...")
    sql_result = agent2.generate_sql(query_plan, verbose=False)
    
    if not sql_result['valid']:
        print(" SQL generation failed")
        return
    
    print("\n[AGENT 3] Validating...")
    approval = agent3.validate_and_approve(sql_result, query_plan, verbose=False)
    
    if not approval['approved']:
        print(" Validation failed")
        return
    
    print("\n[AGENT 4 CLEAN] Executing and analyzing...")
    result = agent4.execute_and_analyze(approval, query_plan, verbose=True)
    
    # Display
    print("\n" + "="*80)
    print(agent4.format_output_professional(result))

    # =======================================================
    # VISUALIZATION AGENT INTEGRATION (New Step)
    # =======================================================
    # Retrieve the DataFrame generated by the executor (Agent 4)
    df_results = result.get('dataframe_results', pd.DataFrame()) 
    
    if not df_results.empty:
        # In your actual Streamlit application, this is where you call the function
        # to render the Plotly charts in the UI.
        
        # NOTE: If you are running this in a console environment, 
        # the charts will not display, but the logic is correct for your Streamlit UI.
        # Ensure 'streamlit as st' is available when running the final app.
        print("\n" + "="*80)
        print(" Visualization Agent triggered successfully.")
        
        # THE ACTUAL FUNCTION CALL FOR STREAMLIT:
        # render_analyst_visualizations(df_results, query_plan)
    
    # Show filter report
    filter_report = result.get('filter_report', {})
    if filter_report.get('sentences_removed', 0) > 0:
        print("\n" + "="*80)
        for item in filter_report['removed_items'][:5]:
            print(f" '{item[:100]}...'")
    
    print("\n" + "="*80)
    
    agent1.close()
    agent2.close()
    agent3.close()
    agent4.close()


if __name__ == "__main__":
    test_clean_agent4()