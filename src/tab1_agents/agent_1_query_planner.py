from tabnanny import verbose
import anthropic
import json
from pathlib import Path
import sys
from typing import Dict, List
import pprint

# Add paths
project_root = Path(__file__).parent.parent 
sys.path.insert(0, str(project_root / 'data_cleaning'))
sys.path.insert(0, str(project_root / 'data_cleaning'/ 'utils'))

from config import Config
from schema_tools import SnowflakeSchemaTools


class QueryPlannerAgent:
    """
    Agent 1: Query Planner (SENTIMENT-AWARE)
    
    Role: Understand user intent and create structured query plan
    Model: Claude Haiku (fast, efficient)
    Tools: RAG search for schema context
    Output: Structured query plan for Agent 2
    
    NEW: Recognizes sentiment analysis queries!
    """
    
    def __init__(self):
        print(" Initializing Agent 1: Query Planner (Sentiment-Aware)...")
        
        # Initialize Claude
        self.client = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
        self.model = Config.CLAUDE_HAIKU
        
        # Initialize tools
        self.schema_tools = SnowflakeSchemaTools()
        
        print(" Query Planner Agent ready!\n")
    
    def plan_query(self, user_query: str, verbose: bool = True) -> Dict:
        """
        Main method: Plan the user's query
        
        Args:
            user_query: Natural language question from user
            verbose: Print progress messages
            
        Returns:
            Dict with query plan
        """
        if verbose:
            print(f" [AGENT 1] Planning query: '{user_query}'")
        
        # ============================================================
        # STEP 0: SCOPE CHECK - Is this an automotive query?
        # ============================================================
        scope_check = self._check_query_scope(user_query)
        
        if not scope_check['in_scope']:
            if verbose:
                print(f"   Query out of scope - not automotive related")
            return {
                "user_query": user_query,
                "in_scope": False,
                "intent": "out_of_scope",
                "error": scope_check['message'],
                "data_sources": [],
                "analysis_type": "none",
                "key_entities": [],
                "filters": {},
                "requires_sentiment": False,
                "query_plan": None,
                "recommended_tables": [],
                "output_requirements": None
            }
        
        if verbose:
            keywords_preview = scope_check.get('keywords_found', [])[:3]
            print(f"   Query in scope (keywords: {keywords_preview})")
        
        # ============================================================
        # STEP 1: Build system prompt
        # ============================================================
        system_prompt = self._get_system_prompt()
        
        # Initial message
        messages = [
            {
                "role": "user",
                "content": f"""User Question: "{user_query}"

Please analyze this question and create a structured query plan. Use the get_snowflake_schema tool to understand what data is available."""
            }
        ]
        
        # ============================================================
        # STEP 2: Agentic loop - let Claude use tools
        # ============================================================
        max_turns = 5
        turn = 0
        
        while turn < max_turns:
            if verbose:
                print(f"   Turn {turn + 1}...")
            
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=self._get_tool_definitions(),
                    messages=messages
                )
            except Exception as e:
                if verbose:
                    print(f"   API Error: {e}")
                return {
                    "user_query": user_query,
                    "in_scope": True,
                    "intent": "error",
                    "error": f"API Error: {str(e)}",
                    "data_sources": [],
                    "analysis_type": "unknown",
                    "key_entities": [],
                    "filters": {},
                    "requires_sentiment": False,
                    "query_plan": None,
                    "recommended_tables": [],
                    "output_requirements": None
                }
            
            # Check if agent wants to use tools
            if response.stop_reason == "tool_use":
                # Process tool calls
                tool_results = self._process_tool_calls(response.content, verbose)
                
                # Add to conversation
                messages.append({
                    "role": "assistant",
                    "content": response.content
                })
                messages.append({
                    "role": "user",
                    "content": tool_results
                })
                
                turn += 1
            else:
                # Agent is done - extract plan
                if verbose:
                    print(f"   Query plan created!\n")
                
                plan = self._extract_query_plan(response, user_query)
                plan['in_scope'] = True
                return plan
        
        # Max turns reached
        if verbose:
            print(f"   Max turns reached\n")
        
        return {
            "user_query": user_query,
            "in_scope": True,
            "intent": "unknown",
            "error": "Max planning turns reached",
            "data_sources": [],
            "analysis_type": "unknown",
            "key_entities": [],
            "filters": {},
            "requires_sentiment": False,
            "query_plan": "Could not create plan",
            "recommended_tables": [],
            "output_requirements": None
        }

    def _check_query_scope(self, user_query: str) -> Dict:
        """Check if query is within automotive domain"""
        
        automotive_keywords = [
            'car', 'cars', 'vehicle', 'vehicles', 'auto', 'automotive', 'automobile',
            'manufacturer', 'make', 'model', 'brand',
            'recall', 'recalls', 'complaint', 'complaints', 'defect', 'defects',
            'safety', 'crash', 'accident', 'injury', 'death', 'fatality',
            'investigation', 'nhtsa', 'rating', 'ratings', 'stars',
            'airbag', 'seatbelt', 'brake', 'brakes', 'steering', 'tire', 'tires',
            'sentiment', 'negative', 'positive', 'customer', 'reputation',
            'fuel', 'mpg', 'economy', 'efficiency', 'emission', 'emissions',
            'epa', 'gas', 'gasoline', 'diesel', 'electric', 'ev', 'hybrid',
            'mileage', 'range', 'battery', 'charging',
            'sales', 'sold', 'market', 'fred', 'economic', 'trend', 'trends',
            'tesla', 'ford', 'toyota', 'honda', 'gm', 'general motors',
            'chevrolet', 'chevy', 'bmw', 'mercedes', 'audi', 'volkswagen', 'vw',
            'hyundai', 'kia', 'nissan', 'subaru', 'mazda', 'lexus', 'acura',
            'infiniti', 'chrysler', 'dodge', 'jeep', 'ram', 'buick', 'cadillac',
            'lincoln', 'volvo', 'porsche', 'jaguar', 'land rover', 'mini',
            'mitsubishi', 'suzuki', 'fiat', 'alfa romeo', 'genesis', 'rivian',
            'lucid', 'polestar', 'ferrari', 'lamborghini', 'maserati',
            'engine', 'transmission', 'drivetrain', 'suspension', 'exhaust',
            'accelerator', 'pedal', 'wheel', 'door', 'window', 'mirror',
            'headlight', 'taillight', 'wiper', 'infotainment'
        ]
        
        query_lower = user_query.lower()
        
        found_keywords = []
        for kw in automotive_keywords:
            if kw in query_lower:
                found_keywords.append(kw)
        
        if len(found_keywords) == 0:
            message = (
                "This query doesn't appear to be related to automotive data.\n\n"
                "AutoInsight can help you analyze:\n"
                "- Vehicle recalls and safety defects (NHTSA)\n"
                "- Consumer complaints with sentiment analysis (210K+ records)\n"
                "- Safety ratings and crash test results\n"
                "- Fuel economy and emissions data (EPA)\n"
                "- Vehicle sales trends (FRED)\n\n"
            )
            return {
                "in_scope": False,
                "keywords_found": [],
                "message": message
            }
        
        return {
            "in_scope": True,
            "keywords_found": found_keywords
        }
    
    def _get_system_prompt(self) -> str:
        """System prompt for Query Planner agent"""
        return """You are Agent 1: Query Planner for AutoInsight, an automotive data analytics system.

Your role is to understand user questions and create structured query plans for SQL generation.

AVAILABLE DATA SOURCES (2015-2025):
1. NHTSA (National Highway Traffic Safety Administration):
   - FACT_RECALLS: Vehicle recall campaigns
   - FACT_COMPLAINTS: Consumer safety complaints WITH SENTIMENT ANALYSIS 
   - FACT_INVESTIGATIONS: Safety defect investigations
   - FACT_RATINGS: 5-star safety crash test ratings

2. EPA (Environmental Protection Agency):
   - FACT_FUEL_ECONOMY: Fuel efficiency, MPG, emissions data

3. FRED (Federal Reserve Economic Data):
   - FACT_ECONOMIC_INDICATORS: Vehicle sales, economic indicators

 SENTIMENT ANALYSIS CAPABILITIES (FACT_COMPLAINTS):
The complaints table includes AI-powered sentiment analysis:
- sentiment_score: -1.0 (very negative) to 1.0 (very positive)
- sentiment_category: VERY_NEGATIVE, NEGATIVE, NEUTRAL, POSITIVE
- safety_keyword_count: Count of safety-related keywords
- negation_count: Count of negation words

SENTIMENT USE CASES:
- Manufacturer reputation: AVG(sentiment_score) by make
- Safety-critical complaints: WHERE sentiment_category = 'VERY_NEGATIVE' AND safety_keyword_count >= 3
- Complaint severity trends: sentiment over time
- Component issues: sentiment by component_description

YOUR TASK:
1. Analyze the user's question
2. Use get_snowflake_schema tool to find relevant tables
3. Identify if user wants sentiment analysis
4. Create a structured query plan in JSON format:

{
  "intent": "Brief description of what user wants",
  "data_sources": ["NHTSA", "EPA", "FRED"],
  "analysis_type": "trend|comparison|aggregation|ranking|correlation|sentiment_analysis",
  "key_entities": ["Tesla", "2024", "recalls"],
  "filters": {
    "manufacturer": "Tesla",
    "year_range": [2015, 2025],
    "sentiment_category": "VERY_NEGATIVE"
  },
  "requires_sentiment": true,
  "query_plan": "Detailed step-by-step plan for SQL generation",
  "recommended_tables": ["NHTSA.FACT_COMPLAINTS"],
  "output_requirements": "What format user expects"
}

EXAMPLES:

User: "How many recalls does Tesla have?"
Intent: Count recalls for specific manufacturer
Analysis type: aggregation
Requires sentiment: false
Tables: ["NHTSA.FACT_RECALLS"]

User: "Which manufacturers have the most negative customer sentiment?"
Intent: Rank manufacturers by sentiment
Analysis type: sentiment_analysis, ranking
Requires sentiment: true
Tables: ["NHTSA.FACT_COMPLAINTS"]
SQL hint: SELECT make, AVG(sentiment_score), COUNT(*) WHERE sentiment_category IN ('NEGATIVE', 'VERY_NEGATIVE')

User: "Show me safety-critical complaints for Honda"
Intent: Find high-severity complaints with safety keywords
Analysis type: sentiment_analysis, filtering
Requires sentiment: true
Tables: ["NHTSA.FACT_COMPLAINTS"]
SQL hint: WHERE make = 'Honda' AND safety_keyword_count >= 3 AND sentiment_category = 'VERY_NEGATIVE'

User: "Compare fuel economy trends over the past 10 years"
Intent: Time series analysis of fuel efficiency
Analysis type: trend
Requires sentiment: false
Tables: ["EPA.FACT_FUEL_ECONOMY"]

User: "Are Tesla complaints getting worse over time?"
Intent: Sentiment trend analysis
Analysis type: sentiment_analysis, trend
Requires sentiment: true
Tables: ["NHTSA.FACT_COMPLAINTS"]
SQL hint: SELECT year, AVG(sentiment_score) WHERE make = 'Tesla' GROUP BY year

IMPORTANT:
- ALWAYS call get_snowflake_schema to verify tables exist
- Recognize sentiment-related keywords: sentiment, negative, positive, reputation, safety-critical, severe
- Data spans 2015-2025 (10 years)
- Be specific about which tables to use

Respond with your analysis and the structured JSON plan."""

    def _get_tool_definitions(self) -> List[Dict]:
        """Tool definitions for Query Planner"""
        return [
            {
                "name": "get_snowflake_schema",
                "description": (
                    "Search for relevant tables in the Snowflake database. "
                    "Use this to verify what data is available for the user's question. "
                    "Returns table names, columns, and descriptions. "
                    "NOTE: FACT_COMPLAINTS includes sentiment analysis columns!"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": (
                                "Search keyword related to user's question. "
                                "Examples: 'recalls', 'safety ratings', 'fuel economy', "
                                "'complaints', 'sentiment', 'economic data'"
                            )
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Number of tables to return (1-5)",
                            "default": 3
                        }
                    },
                    "required": ["keyword"]
                }
            }
        ]
    
    def _process_tool_calls(self, content: List, verbose: bool) -> List[Dict]:
        """Execute tool calls from Claude"""
        tool_results = []
        
        for block in content:
            if hasattr(block, 'type') and block.type == "tool_use":
                tool_name = block.name
                tool_input = block.input
                
                if verbose:
                    print(f"       Calling tool: {tool_name}({tool_input.get('keyword', '')})")
                
                # Execute tool
                if tool_name == "get_snowflake_schema":
                    result = self.schema_tools.get_snowflake_schema(
                        keyword=tool_input['keyword'],
                        top_k=tool_input.get('top_k', 3)
                    )
                else:
                    result = {"error": f"Unknown tool: {tool_name}"}
                
                # Format for Claude
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result
                })
        
        return tool_results
    
    def _extract_query_plan(self, response, user_query: str) -> Dict:
        """Extract structured query plan from Claude's response"""
        
        # Get text from response
        text_content = ""
        for block in response.content:
            if hasattr(block, 'text'):
                text_content += block.text
        
        # Try to extract JSON from response
        try:
            import re
            json_match = re.search(r'\{[\s\S]*\}', text_content)
            
            if json_match:
                plan_json = json.loads(json_match.group())
                
                # Ensure required fields
                plan = {
                    "user_query": user_query,
                    "intent": plan_json.get("intent", "Unknown"),
                    "data_sources": plan_json.get("data_sources", []),
                    "analysis_type": plan_json.get("analysis_type", "unknown"),
                    "key_entities": plan_json.get("key_entities", []),
                    "filters": plan_json.get("filters", {}),
                    "requires_sentiment": plan_json.get("requires_sentiment", False),
                    "query_plan": plan_json.get("query_plan", text_content),
                    "recommended_tables": plan_json.get("recommended_tables", []),
                    "output_requirements": plan_json.get("output_requirements", "tabular"),
                    "raw_response": text_content
                }
                
                return plan
        except json.JSONDecodeError:
            pass
        
        # Fallback
        return {
            "user_query": user_query,
            "intent": "Could not extract structured plan",
            "query_plan": text_content,
            "raw_response": text_content
        }
    
    def close(self):
        """Cleanup resources"""
        self.schema_tools.close()


# ============================================================
# TEST AGENT 1 WITH SENTIMENT QUERIES
# ============================================================

def test_agent_1():
    """Test Query Planner with sentiment-aware queries"""
    
    print("="*80)
    print(" Testing Agent 1: Query Planner (Sentiment-Aware)")
    print("="*80)
    print()
    
    agent = QueryPlannerAgent()
    
    # Test queries - including sentiment!
    test_queries = [
        "How many recalls does Ford have?",
        "Which manufacturers have the worst customer sentiment?",
        "Show me safety-critical complaints with high safety keyword counts",
        "Compare Tesla sentiment vs Honda sentiment",
        "What are the top 5 manufacturers by recall count?",
        "Are complaints for electric vehicles more negative than gas vehicles?"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}: {query}")
        print("="*80)
        
        plan = agent.plan_query(query, verbose=True)
        
        print(f"\n QUERY PLAN:")
        print(f"   Intent: {plan.get('intent', 'N/A')}")
        print(f"   Data Sources: {plan.get('data_sources', [])}")
        print(f"   Analysis Type: {plan.get('analysis_type', 'N/A')}")
        print(f"   Requires Sentiment: {plan.get('requires_sentiment', False)}")
        print(f"   Recommended Tables: {plan.get('recommended_tables', [])}")
        print()
    
    print("="*80)
    print(" Agent 1 Testing Complete!")
    print("="*80)
    
    agent.close()


if __name__ == "__main__":
    test_agent_1()