"""
AutoInsight - Complete Unified System
- Tab 1: Query Agent with HITL approval workflow
- Tab 2: News & Events Portal  
- Full RBAC with approval queue and request tracking
"""

import streamlit as st
import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import requests
from typing import Dict, Optional, List, Any
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# FIX IMPORTS
# ============================================================

project_root = Path(__file__).parent.absolute()

paths_to_add = [
    str(project_root),
    str(project_root / 'src'),
    str(project_root / 'src' / 'auth'),
    str(project_root / 'src' / 'tab1_agents'),
    str(project_root / 'src' / 'data_cleaning' / 'utils'),
    str(project_root / 'src' / 'tools'),
    str(project_root / 'src' / 'rag'),
]

for path in paths_to_add:
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ['PYTHONPATH'] = ':'.join(paths_to_add)

# ============================================================
# IMPORT MODULES
# ============================================================

import_errors = []

try:
    from auth.user_management import UserManager, RBACRules
except ImportError as e:
    import_errors.append(f"user_management: {e}")

try:
    from tab1_agents.agent_1_query_planner import QueryPlannerAgent
except ImportError as e:
    import_errors.append(f"agent_1_query_planner: {e}")

try:
    from tab1_agents.agent_2_sql_generator import SQLGeneratorWithRBAC
except ImportError as e:
    import_errors.append(f"agent_2_sql_generator: {e}")

try:
    from tab1_agents.agent_3_validator import ValidatorAgent
except ImportError as e:
    import_errors.append(f"agent_3_validator: {e}")

try:
    from tab1_agents.agent_4_executor_analyzer import ExecutorAnalyzerClean
except ImportError as e:
    import_errors.append(f"agent_4_executor_analyzer: {e}")

try:
    # Assuming you saved visualization_agent.py in the 'tab1_agents' directory
    from tab1_agents.agent_5_vis import render_analyst_visualizations
except ImportError as e:
    import_errors.append(f"visualization_agent: {e}")

if import_errors:
    st.error("Import Errors Detected")
    for error in import_errors:
        st.code(error)
    st.stop()

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="AutoInsight | Automotive Intelligence Platform",
    page_icon="📊",
    layout="wide"
)

# Professional CSS (Combined from both files)
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6b7280;
        margin-bottom: 2rem;
    }
    .access-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 0.25rem;
        font-size: 0.875rem;
        font-weight: 500;
    }
    .badge-analyst { background-color: #dbeafe; color: #1e40af; }
    .badge-manager { background-color: #fef3c7; color: #92400e; }
    .badge-executive { background-color: #d1fae5; color: #065f46; }
    
    .approval-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        background: #fef3c7;
        color: #92400e;
        font-weight: 600;
    }
    
    .metric-card {
        background-color: #f9fafb;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
    }
    
    .metric-box {
        background: #f9fafb;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
    }
    
    .insight-box {
        background-color: #f0f9ff;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #3b82f6;
        margin: 0.5rem 0;
    }
    
    .warning-box {
        background-color: #fef3c7;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #f59e0b;
        margin: 0.5rem 0;
    }
    
    .news-card {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
        margin-bottom: 1rem;
        transition: box-shadow 0.2s;
    }
    
    .news-card:hover {
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    .event-card {
        background-color: #fafafa;
        padding: 1.25rem;
        border-radius: 0.5rem;
        border-left: 4px solid #8b5cf6;
        margin-bottom: 1rem;
    }
    
    .sentiment-positive { color: #10b981; font-weight: 600; }
    .sentiment-negative { color: #ef4444; font-weight: 600; }
    .sentiment-neutral { color: #6b7280; font-weight: 500; }
    
    .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 8px;
    }
    
    .status-success { background-color: #10b981; }
    .status-warning { background-color: #f59e0b; }
    .status-error { background-color: #ef4444; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# SESSION STATE
# ============================================================

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user' not in st.session_state:
    st.session_state.user = None
if 'user_manager' not in st.session_state:
    st.session_state.user_manager = UserManager()
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'query'

# MCP Server URL
MCP_URL = os.getenv("MCP_SERVER_URL", "http://backend:8001")

# ============================================================
# TAB 1 FUNCTIONS
# ============================================================

def process_query_complete(user_query: str, user: Dict):
    """Complete query processing with proper HITL handling"""
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    try:
        start_time = time.time()
        
        # Agent 1
        status_container.text("Agent 1: Understanding question...")
        progress_bar.progress(20)
        query_plan = st.session_state.agent1.plan_query(user_query, verbose=False)
        
        # ============================================================
        # CHECK IF OUT OF SCOPE - ADD THIS BLOCK
        # ============================================================
        if not query_plan.get('in_scope', True):
            progress_bar.empty()
            status_container.empty()
            
            st.warning("Query Outside AutoInsight's Scope")
            st.info(query_plan.get('error', 'This query is not related to automotive data.'))
            return
        
        # ============================================================
        # REST OF YOUR EXISTING CODE CONTINUES HERE
        # ============================================================
        
        # Agent 2
        status_container.text("Agent 2: Generating SQL...")
        progress_bar.progress(40)
        sql_result = st.session_state.agent2.generate_sql(query_plan, verbose=False)
        
        # Check access denial
        if sql_result.get('access_denied'):
            progress_bar.empty()
            status_container.empty()
            
            st.error("Access Denied")
            st.warning(sql_result['error'])
            
            if sql_result.get('blocked_columns'):
                st.markdown("**Restricted Data:**")
                for col in sql_result['blocked_columns']:
                    st.markdown(f"- {col}")
                st.markdown(f"**Required Level:** {sql_result.get('required_tier', 'Higher')}")
            
            return
        
        if not sql_result['valid']:
            progress_bar.empty()
            status_container.empty()
            
            st.error("Failed to generate valid SQL")
            st.warning(sql_result.get('error', 'Please try rephrasing your question'))
            
            with st.expander("Debug Info"):
                st.json(sql_result)
            
            return
        
        # Agent 3
        status_container.text("Agent 3: Validating...")
        progress_bar.progress(60)
        approval = st.session_state.agent3.validate_and_approve(
            sql_result, query_plan, verbose=False
        )
        
        # HITL CHECK
        if approval.get('hitl_required'):
            progress_bar.empty()
            status_container.empty()
            
            # Submit to queue
            approval_id = st.session_state.user_manager.submit_for_approval(
                requester_user_id=user['user_id'],
                requester_username=user['username'],
                requester_tier=user['user_tier'],
                query_text=user_query,
                sql_generated=sql_result['sql'],
                estimated_cost=approval['estimated_cost'],
                estimated_rows=approval['estimated_rows'],
                risk_level=approval['risk_level'],
                hitl_reason=approval['hitl_reason']
            )
            
            # Clear previous results
            if 'last_result' in st.session_state:
                del st.session_state.last_result
            
            # Display approval required screen
            st.markdown("---")
            st.markdown("## Approval Required")
            
            approver = "Manager or Executive" if user['user_tier'] == 'analyst' else "Executive"
            
            st.warning(f"This query requires {approver} approval")
            
            st.markdown(f"**Request ID:** #{approval_id}")
            st.info(approval['hitl_reason'][:300] + "..." if len(approval['hitl_reason']) > 300 else approval['hitl_reason'])
            
            # Metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Estimated Cost", f"${approval['estimated_cost']:.2f}")
            with col2:
                st.metric("Estimated Rows", f"{approval['estimated_rows']:,}")
            with col3:
                st.metric("Risk Level", approval['risk_level'].upper())
            
            # SQL
            with st.expander("View Generated SQL"):
                st.code(sql_result['sql'], language='sql')
            
            # Status
            st.success(f"Request #{approval_id} submitted to approval queue")
            
            # Navigation
            st.markdown("---")
            st.markdown("### What's Next?")
            
            col_nav1, col_nav2 = st.columns(2)
            
            with col_nav1:
                if st.button("Submit Another Query", use_container_width=True, type="primary"):
                    st.rerun()
            
            with col_nav2:
                if st.button("Ask for Approval", use_container_width=True):
                    st.session_state.logged_in = False
                    st.session_state.user = None
                    st.info(f"Logout successful. Login as {approver} to approve this request.")
                    time.sleep(2)
                    st.rerun()
            
            return
        
        # No HITL - execute directly
        status_container.text("Agent 4: Executing and analyzing...")
        progress_bar.progress(80)
        
        result = st.session_state.agent4.execute_and_analyze(
            approval, query_plan, verbose=False
        )
        
        execution_time = time.time() - start_time
        
        progress_bar.empty()
        status_container.empty()
        
        if result['success']:
            # Store result
            result['execution_time_seconds'] = execution_time
            result['query_plan'] = query_plan
            st.session_state.last_result = result
            
        else:
            st.error(f"Execution failed: {result.get('error')}")
    
    except Exception as e:
        progress_bar.empty()
        status_container.empty()
        st.error(f"Error: {e}")
        
        with st.expander("Details"):
            import traceback
            st.code(traceback.format_exc())



def display_hitl_submission(approval: Dict, approval_id: int, user: Dict):
    """Display HITL submission confirmation"""
    
    st.markdown("---")
    st.markdown("## Approval Required")
    
    approver = "Manager or Executive" if user['user_tier'] == 'analyst' else "Executive"
    
    st.warning(f"🔔 This query requires {approver} approval")
    
    st.markdown(f"**Request ID:** #{approval_id}")
    st.info(approval['hitl_reason'])
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Estimated Cost", f"${approval.get('estimated_cost', 0):.2f}")
    with col2:
        st.metric("Estimated Rows", f"{approval.get('estimated_rows', 0):,}")
    with col3:
        st.metric("Risk Level", approval.get('risk_level', 'medium').upper())
    
    with st.expander("View Generated SQL"):
        st.code(approval.get('sql', ''), language='sql')
    
    st.success(f"✅ Request #{approval_id} submitted to approval queue")
    
    # Navigation
    st.markdown("---")
    st.markdown("### What's Next?")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("← Submit Another Query", use_container_width=True, type="primary"):
            st.rerun()
    
    with col2:
        if st.button("View My Requests", use_container_width=True):
            st.session_state.current_page = 'my_requests'
            st.rerun()


def display_request_card(request: Dict, status: str):
    """Display a request card with status"""
    
    status_icons = {
        'pending': '🟡',
        'approved': '🟢',
        'rejected': '🔴'
    }
    
    with st.container():
        st.markdown(f"#### {status_icons.get(status, '')} Request #{request['approval_id']}")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Status", request['status'].title())
        with col2:
            st.metric("Cost", f"${request['estimated_cost']:.2f}")
        with col3:
            st.metric("Risk", request['risk_level'].upper())
        
        st.markdown(f"**Query:** {request['query_text']}")
        st.caption(f"Submitted: {request['created_at']}")
        
        if request['status'] == 'approved' and request.get('approver_username'):
            st.success(f"✅ Approved by: {request['approver_username']} on {request.get('reviewed_at', 'N/A')}")
            
            if request.get('approval_notes'):
                with st.expander("View Results"):
                    st.info(request['approval_notes'])
        
        elif request['status'] == 'rejected':
            st.error(f"❌ Rejected by: {request.get('approver_username', 'Unknown')}")
            if request.get('approval_notes'):
                st.warning(f"Reason: {request['approval_notes']}")
        
        with st.expander("View SQL"):
            st.code(request['sql_generated'], language='sql')
        
        st.markdown("---")


import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Dict
# Note: You must ensure 'render_analyst_visualizations' is imported at the top of streamlit_app.py

def display_results_with_dashboard(result: Dict, user: Dict, execution_time: float):
    """Display results with complete dashboard metrics and Role-Based Access Control (RBAC) check."""
    
    # =======================================================
    # 💡 NEW: ROLE-BASED ACCESS CONTROL (RBAC) CHECK
    # =======================================================
    
    query_plan = result.get('query_plan', {})
    # Default required tier to 'analyst' if not explicitly set in the plan
    required_tier = query_plan.get('required_tier', 'analyst').lower() 
    # Default current tier to 'analyst' if missing
    current_tier = user.get('user_tier', 'analyst').lower() 

    # Define tier hierarchy: a higher index means more access
    tier_order = {'analyst': 1, 'manager': 2, 'executive': 3}
    required_index = tier_order.get(required_tier, 1)
    current_index = tier_order.get(current_tier, 1)

    # Check if the current user meets the tier requirement
    if current_index < required_index:
        st.markdown("---")
        st.error(f"⚠️ **ACCESS DENIED**\n\nThe underlying data for this analysis requires **{required_tier.capitalize()}** access. Your current tier is **{current_tier.capitalize()}**. This analysis cannot be viewed.")
        return # STOP rendering the dashboard

    # =======================================================
    # END RBAC CHECK
    # =======================================================

    st.markdown("---")
    st.markdown("## Analysis Results")
    
    # DASHBOARD METRICS (4 columns)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-box">', unsafe_allow_html=True)
        st.metric("Total Rows", f"{result.get('row_count', 0):,}")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-box">', unsafe_allow_html=True)
        st.metric("Columns", len(result.get('columns', [])))
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-box">', unsafe_allow_html=True)
        # Count tables from SQL
        sql = result.get('sql', '')
        tables = len(set([t for t in ['FACT_RECALLS', 'FACT_COMPLAINTS', 'FACT_RATINGS', 
                                     'FACT_FUEL_ECONOMY', 'FACT_INVESTIGATIONS', 
                                     'FACT_ECONOMIC_INDICATORS'] if t in sql.upper()]))
        st.metric("Tables Queried", tables)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-box">', unsafe_allow_html=True)
        st.metric("Execution Time", f"{execution_time:.2f}s")
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Tabs for organized display
    tab_viz, tab_analysis, tab_data, tab_sql = st.tabs(["📊 Visualizations", "💬 Analysis", "💾 Data", "💻 SQL Query"])
    
    with tab_viz:
        # NEW CODE: Call the Visualization Agent here (Agent 5)
        st.markdown("### Primary Visualization")
        
        df_results = pd.DataFrame(result.get('data', {}))
        query_plan = result.get('query_plan', {})
        
        if not df_results.empty:
            render_analyst_visualizations(df_results, query_plan)
        else:
            st.info("No data returned to generate visualizations.")
            
    with tab_analysis:
        # Executive Summary
        if result.get('executive_summary'):
            st.markdown("### Executive Summary")
            st.info(result['executive_summary'])
        
        # Key Insights
        if result.get('key_insights'):
            st.markdown("### Key Insights")
            for i, insight in enumerate(result['key_insights'], 1):
                st.markdown(f"{i}. {insight}")
        
        # Comparisons
        if result.get('comparisons'):
            st.markdown("### Comparisons")
            for comp in result['comparisons']:
                st.markdown(f"- {comp}")
        
        # Recommendations
        if result.get('recommendations'):
            st.markdown("### Recommendations")
            for i, rec in enumerate(result['recommendations'], 1):
                st.markdown(f"{i}. {rec}")
    
    with tab_data:
        st.markdown("### Query Results")
        
        if result.get('data'):
            df = pd.DataFrame(result['data'])
            
            # Format numeric columns
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32']:
                    df[col] = df[col].round(3)
            
            # Display table
            st.dataframe(df, use_container_width=True, height=500)
            
            # DOWNLOAD BUTTON
            st.markdown("---")
            st.markdown("### Export Data")
            
            csv = df.to_csv(index=False)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            st.download_button(
                label="Download Results as CSV",
                data=csv,
                file_name=f"autoinsight_results_{timestamp}.csv",
                mime="text/csv",
                key='download_results',
                use_container_width=False
            )
        else:
            st.info("No data returned")
    
    with tab_sql:
        st.markdown("### Generated SQL Query")
        st.code(result.get('sql', ''), language='sql')
        
        # Execution metadata
        exec_time = result.get('execution_time', '')
        if exec_time:
            try:
                # #Note: The result dict has 'execution_time' as a string timestamp, 
                # but the function parameter is the float duration.
                # Assuming the result['execution_time'] key holds the timestamp.
                dt = datetime.fromisoformat(exec_time)
                st.caption(f"Executed: {dt.strftime('%B %d, %Y at %I:%M:%S %p')}")
            except:
                pass
        
        st.caption(f"Execution Time: {execution_time:.2f} seconds")


# ============================================================
# TAB 2 FUNCTIONS
# ============================================================

def call_mcp_tool(endpoint: str, payload: dict) -> dict:
    """Call MCP server tool"""
    try:
        response = requests.post(
            f"{MCP_URL}/mcp/tool/{endpoint}",
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"success": False, "error": str(e)}


def format_date(date_str: str) -> str:
    """Format date"""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime("%B %d, %Y")
    except:
        return date_str


def format_datetime(date_str: str) -> str:
    """Format datetime"""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime("%b %d, %Y at %I:%M %p")
    except:
        return date_str


def get_sentiment_display(sentiment: str) -> tuple:
    """Get sentiment emoji and CSS class"""
    sentiment_map = {
        "positive": ("✅", "sentiment-positive"),
        "negative": ("⚠️", "sentiment-negative"),
        "neutral": ("➖", "sentiment-neutral")
    }
    return sentiment_map.get(sentiment.lower(), ("➖", "sentiment-neutral"))

def render_article_dashboard(dashboard: Dict):
    """
    Render the 4-section AI-generated dashboard for an article
    Called after MCP returns the dashboard from Claude
    """
    
    # Custom CSS for this dashboard
    st.markdown("""
    <style>
        .dashboard-header {
            font-size: 1.1rem !important;
            font-weight: 600 !important;
            color: #000000 !important;
            margin-bottom: 0.5rem !important;
        }
        .dashboard-text {
            color: #000000 !important;
        }
        .dashboard-caption {
            color: #000000 !important;
            font-size: 0.85rem !important;
        }
        .entity-tag {
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            margin: 2px;
            display: inline-block;
        }
        .flag-yes {
            background: #fee2e2;
            color: #dc2626;
            padding: 6px 12px;
            border-radius: 6px;
            margin: 4px 0;
            font-size: 12px;
        }
        .flag-no {
            background: #f9fafb;
            color: #374151;
            padding: 6px 12px;
            border-radius: 6px;
            margin: 4px 0;
            font-size: 12px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Create 4 columns with reduced gap
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1.2], gap="small")
    
    # =========================================================
    # SECTION 1: KEY ENTITIES
    # =========================================================
    with col1:
        st.markdown('<p class="dashboard-header"> Key Entities</p>', unsafe_allow_html=True)
        
        entities = dashboard.get('key_entities', {})
        
        # Manufacturers
        manufacturers = entities.get('manufacturers', [])
        if manufacturers:
            st.markdown('<p class="dashboard-caption"><strong>Manufacturers</strong></p>', unsafe_allow_html=True)
            for m in manufacturers[:5]:
                st.markdown(f"<span class='entity-tag' style='background:#dbeafe;color:#1e40af;'>{m}</span>", unsafe_allow_html=True)
        
        # Models
        models = entities.get('models', [])
        if models:
            st.markdown('<p class="dashboard-caption"><strong>Models</strong></p>', unsafe_allow_html=True)
            for m in models[:5]:
                st.markdown(f"<span class='entity-tag' style='background:#fef3c7;color:#92400e;'>{m}</span>", unsafe_allow_html=True)
        
        # Executives
        executives = entities.get('executives', [])
        if executives:
            st.markdown('<p class="dashboard-caption"><strong>Executives</strong></p>', unsafe_allow_html=True)
            for e in executives[:3]:
                st.markdown(f"<span class='entity-tag' style='background:#f3e8ff;color:#7c3aed;'>{e}</span>", unsafe_allow_html=True)
        
        # Locations/Regions
        locations = entities.get('locations', []) or entities.get('regions', [])
        if locations:
            st.markdown('<p class="dashboard-caption"><strong>Locations</strong></p>', unsafe_allow_html=True)
            st.markdown(f'<p class="dashboard-text">{", ".join(locations[:3])}</p>', unsafe_allow_html=True)
        
        if not any([manufacturers, models, executives, locations]):
            st.markdown('<p class="dashboard-text">No entities extracted</p>', unsafe_allow_html=True)
    
    # =========================================================
    # SECTION 2: CLASSIFICATION
    # =========================================================
    with col2:
        st.markdown('<p class="dashboard-header">Classification</p>', unsafe_allow_html=True)
        
        classification = dashboard.get('classification', {})
        
        # Category
        category = classification.get('category', 'General')
        st.markdown(f'<p class="dashboard-caption"><strong>Category</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p class="dashboard-text" style="font-size:1.2rem;font-weight:500;">{category}</p>', unsafe_allow_html=True)
        
        # Sentiment - smaller icon
        sentiment = classification.get('sentiment', 'Neutral')
        sentiment_icons = {
            'positive': '🟢',
            'negative': '🔴',
            'neutral': '⚪',
            'mixed': '🟡'
        }
        icon = sentiment_icons.get(sentiment.lower(), '⚪')
        st.markdown(f'<p class="dashboard-caption"><strong>Sentiment</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p class="dashboard-text"><span style="font-size:0.7rem;">{icon}</span> {sentiment.title()}</p>', unsafe_allow_html=True)
        
        # Impact Score
        impact = dashboard.get('impact_assessment', {})
        severity = impact.get('severity', 5)
        st.markdown(f'<p class="dashboard-caption"><strong>Impact Score</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p class="dashboard-text" style="font-size:1.2rem;font-weight:500;">{severity}/10</p>', unsafe_allow_html=True)
        st.progress(severity / 10)
        
        # Reasoning
        importance_reasoning = classification.get('importance_reasoning', '')
        if not importance_reasoning:
            # Try to get from impact assessment
            time_horizon = impact.get('time_horizon', '')
            if time_horizon:
                importance_reasoning = f"Time horizon: {time_horizon}"
        
        if importance_reasoning:
            st.markdown(f'<p class="dashboard-text" style="font-style:italic;font-size:0.85rem;">{importance_reasoning}</p>', unsafe_allow_html=True)
    
    # =========================================================
    # SECTION 3: IMPACT FLAGS
    # =========================================================
    with col3:
        st.markdown('<p class="dashboard-header"> Impact Flags</p>', unsafe_allow_html=True)
        
        flags = dashboard.get('impact_flags', {})
        
        flag_config = [
            ('breaking_news', ' Breaking News'),
            ('recall_related', ' Recall Related'),
            ('affects_stock', ' Affects Stock'),
            ('affects_guidance', ' Affects Guidance'),
            ('supply_chain_disruption', ' Supply Chain'),
            ('regulatory_action', ' Regulatory'),
            ('labor_action', ' Labor Impact'),
            ('merger_acquisition', ' M&A Activity')
        ]
        
        for flag_key, flag_label in flag_config:
            value = flags.get(flag_key, False)
            if value:
                st.markdown(f"<div class='flag-yes'>{flag_label}: <strong>YES</strong></div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='flag-no'>{flag_label}: NO</div>", unsafe_allow_html=True)
    
    # =========================================================
    # SECTION 4: EXECUTIVE SUMMARY
    # =========================================================
    with col4:
        st.markdown('<p class="dashboard-header">Executive Summary</p>', unsafe_allow_html=True)
        
        summary = dashboard.get('executive_summary', {})
        
        # Headline
        headline = summary.get('headline', '')
        if headline:
            st.markdown(f'<p class="dashboard-text" style="font-weight:600;">{headline}</p>', unsafe_allow_html=True)
        
        # Summary text (situation)
        situation = summary.get('situation', '') or summary.get('summary', 'No summary available')
        st.markdown(f'<p class="dashboard-text">{situation}</p>', unsafe_allow_html=True)
        
        # Key Takeaways (from key_figures or action_items)
        takeaways = summary.get('key_figures', []) or summary.get('key_takeaways', [])
        if takeaways:
            st.markdown('<p class="dashboard-caption"><strong>Key Takeaways:</strong></p>', unsafe_allow_html=True)
            for t in takeaways[:4]:
                st.markdown(f'<p class="dashboard-text" style="font-size:0.85rem;">• {t}</p>', unsafe_allow_html=True)
        
        # Implications
        implications = summary.get('implications', '') or summary.get('investment_implications', '')
        if implications:
            st.markdown('<p class="dashboard-caption"><strong>Investment Note:</strong></p>', unsafe_allow_html=True)
            st.markdown(f'<p class="dashboard-text" style="font-style:italic;font-size:0.85rem;">{implications}</p>', unsafe_allow_html=True)
def render_event_dashboard(dashboard: Dict):
    """
    Render AI-generated dashboard for an event
    """

    # Add custom CSS for black text
    st.markdown("""
    <style>
        /* Event dashboard text colors */
        .element-container p, 
        .element-container span,
        .element-container div,
        .stMarkdown p {
            color: #000000 !important;
        }
    </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    # Section 1: Key Entities
    with col1:
        st.markdown("#####  Key Entities")
        
        entities = dashboard.get('key_entities', {})
        
        manufacturers = entities.get('manufacturers', [])
        if manufacturers:
            st.caption("**Expected Manufacturers**")
            for m in manufacturers[:5]:
                st.markdown(f"<span style='background:#dbeafe;color:#1e40af;padding:2px 8px;border-radius:4px;font-size:12px;margin:2px;display:inline-block;'>{m}</span>", unsafe_allow_html=True)
        
        organizations = entities.get('organizations', [])
        if organizations:
            st.caption("**Organizations**")
            for o in organizations[:3]:
                st.caption(f"• {o}")
    
    # Section 2: Classification & Importance
    with col2:
        st.markdown("#####  Classification")
        
        # Event profile (this is a dict in your Pydantic model)
        event_profile = dashboard.get('event_profile', {})
        event_type = event_profile.get('event_type', 'Conference')
        tier = event_profile.get('tier', 'Tier 2')
        
        #st.metric("Event Type", event_type.title())
        st.markdown(f"<p style='margin:0;padding:0;font-size:0.75rem;color:#6b7280;'>Event Type</p>", unsafe_allow_html=True)
        st.markdown(f"<p style='margin:0 0 1rem 0;font-size:1rem;font-weight:500;'>{event_type.title()}</p>", unsafe_allow_html=True)
        st.metric("Tier", tier)
        
        # Strategic importance (this is ALSO a dict in your Pydantic model!)
        strategic_importance = dashboard.get('strategic_importance', {})
        importance_level = strategic_importance.get('importance_level', 'Medium')
        relevance_score = strategic_importance.get('relevance_score', 0.5)
        
        st.metric("Importance", importance_level.title())
        st.metric("Relevance", f"{relevance_score:.1f}")
    
    # Section 3: Summary & Details
    with col3:
        st.markdown("##### Summary")
        
        # Analyst brief (this is a dict)
        analyst_brief = dashboard.get('analyst_brief', {})
        
        summary_text = analyst_brief.get('summary', 'No summary available')
        st.write(summary_text)
        
        # Expected coverage (this is a dict)
        expected_coverage = dashboard.get('expected_coverage', {})
        announcements = expected_coverage.get('potential_market_movers', [])
        
        if announcements:
            st.caption("**Expected Announcements:**")
            for a in announcements[:3]:
                st.caption(f"• {a}")
        
# ============================================================
# LOGIN PAGES
# ============================================================

def login_page():
    """Login page"""
    
    st.markdown('<h1 class="main-header">AutoInsight</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Automotive Intelligence Platform</p>', unsafe_allow_html=True)
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.subheader("Sign In")
        
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            col1, col2 = st.columns(2)
            
            with col1:
                submit = st.form_submit_button("Sign In", type="primary", use_container_width=True)
            with col2:
                signup = st.form_submit_button("Create Account", use_container_width=True)
            
            if signup:
                st.session_state.show_signup = True
                st.rerun()
            
            if submit:
                if username and password:
                    user = st.session_state.user_manager.authenticate(username, password)
                    if user:
                        st.session_state.logged_in = True
                        st.session_state.user = user
                        st.success(f"Welcome, {user['full_name']}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Invalid credentials")
        
        if st.session_state.user_manager.get_user_count() == 0:
            st.info("No users found. Create an account to get started.")


def signup_page():
    """Signup page"""
    
    st.markdown('<h1 class="main-header">AutoInsight</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Create Account</p>', unsafe_allow_html=True)
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("signup_form"):
            username = st.text_input("Username")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            password_confirm = st.text_input("Confirm Password", type="password")
            full_name = st.text_input("Full Name")
            organization = st.text_input("Organization")
            
            user_tier = st.selectbox(
                "Role",
                ["analyst", "manager", "executive"],
                format_func=lambda x: {
                    "analyst": "Analyst - Aggregated metrics",
                    "manager": "Manager - Detailed analysis",
                    "executive": "Executive - Full access"
                }[x]
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                submit = st.form_submit_button("Create Account", type="primary", use_container_width=True)
            with col2:
                back = st.form_submit_button("Back", use_container_width=True)
            
            if back:
                st.session_state.show_signup = False
                st.rerun()
            
            if submit:
                if not username or not password or len(password) < 6:
                    st.error("Fill all fields (min 6 char password)")
                elif password != password_confirm:
                    st.error("Passwords don't match")
                else:
                    result = st.session_state.user_manager.create_user(
                        username, email, password, user_tier,
                        full_name or username, organization or "AutoInsight"
                    )
                    
                    if result['success']:
                        st.success("Account created!")
                        time.sleep(1)
                        st.session_state.show_signup = False
                        st.rerun()
                    else:
                        st.error(result['error'])

# ============================================================
# APPROVAL QUEUE PAGE
# ============================================================

def approval_queue_page():
    """Approval queue for managers/executives"""
    
    user = st.session_state.user
    
    st.markdown('<h1 class="main-header">Approval Queue</h1>', unsafe_allow_html=True)
    st.markdown(f"**{user['full_name']}** | {user['user_tier'].title()}")
    st.markdown("---")
    
    # Initialize agents if not done
    if 'agents_initialized' not in st.session_state:
        with st.spinner("Initializing agents for query execution..."):
            try:
                st.session_state.agent1 = QueryPlannerAgent()
                st.session_state.agent2 = SQLGeneratorWithRBAC(user_tier=user['user_tier'])
                st.session_state.agent3 = ValidatorAgent(user_tier=user['user_tier'])
                st.session_state.agent4 = ExecutorAnalyzerClean()
                st.session_state.agents_initialized = True
            except Exception as e:
                st.error(f"Failed to initialize agents: {e}")
                return
    
    # Navigation back
    if st.button("← Back to Query Interface"):
        st.session_state.current_page = 'query'
        # Clear any approval results when leaving
        if 'last_approval_result' in st.session_state:
            del st.session_state.last_approval_result
        st.rerun()
    
    st.markdown("---")
    
    # =========================================================
    # SHOW LAST APPROVAL RESULT (if exists)
    # =========================================================
    if 'last_approval_result' in st.session_state:
        result = st.session_state.last_approval_result
        
        st.success(f"✅ Request #{result['approval_id']} approved and executed!")
        
        with st.expander("📊 View Execution Results", expanded=True):
            st.markdown(f"**Query:** {result['query_text']}")
            st.markdown(f"**Requester:** {result['requester_username']}")
            st.markdown(f"**Rows Returned:** {result['row_count']:,}")
            
            if result.get('data'):
                df = pd.DataFrame(result['data'])
                
                # Format numeric columns
                for col in df.columns:
                    if df[col].dtype in ['float64', 'float32']:
                        df[col] = df[col].round(3)
                
                st.dataframe(df, use_container_width=True, height=400)
                
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    f"approved_query_{result['approval_id']}.csv",
                    key=f"download_last_approval_{result['approval_id']}",
                    use_container_width=False
                )
        
        # Clear button
        if st.button("✖️ Dismiss Results", use_container_width=False):
            del st.session_state.last_approval_result
            st.rerun()
        
        st.markdown("---")
    
    # =========================================================
    # PENDING APPROVALS
    # =========================================================
    pending = st.session_state.user_manager.get_pending_approvals(user['user_tier'])
    
    if not pending:
        st.info(" No pending approval requests")
        return
    
    st.markdown(f"### Pending Requests: {len(pending)}")
    
    # Display each request
    for approval in pending:
        with st.container():
            st.markdown(f"#### Request #{approval['approval_id']} from {approval['requester_username']}")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Cost", f"${approval['estimated_cost']:.2f}")
            with col2:
                st.metric("Rows", f"{approval['estimated_rows']:,}")
            with col3:
                st.metric("Risk", approval['risk_level'].upper())
            with col4:
                st.metric("Requester", approval['requester_tier'].title())
            
            st.markdown(f"**Query:** {approval['query_text']}")
            st.caption(f"Submitted: {approval['created_at']}")
            
            with st.expander("⚠️ Why Approval Required"):
                st.warning(approval['hitl_reason'])
            
            with st.expander("🔍 View SQL Query"):
                st.code(approval['sql_generated'], language='sql')
            
            st.markdown("---")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("✅ Approve & Execute", key=f"approve_{approval['approval_id']}", type="primary", use_container_width=True):
                    
                    with st.spinner("Executing approved query..."):
                        try:
                            execution_result = st.session_state.agent4.schema_tools.execute_snowflake_query(
                                approval['sql_generated'], limit=10000
                            )
                            
                            if execution_result['success']:
                                results_summary = f"Executed successfully. Returned {execution_result['row_count']} rows."
                                
                                success = st.session_state.user_manager.approve_query(
                                    approval['approval_id'], 
                                    user['user_id'], 
                                    user['username'],
                                    execution_results=results_summary
                                )
                                
                                if success:
                                    # Cache results for display
                                    st.session_state.last_approval_result = {
                                        'approval_id': approval['approval_id'],
                                        'query_text': approval['query_text'],
                                        'requester_username': approval['requester_username'],
                                        'row_count': execution_result['row_count'],
                                        'data': execution_result['data']
                                    }
                                    st.rerun()  # Now results will persist after rerun
                                else:
                                    st.error("Failed to update approval status")
                            else:
                                st.error(f"Execution failed: {execution_result.get('error')}")
                                
                                st.session_state.user_manager.approve_query(
                                    approval['approval_id'], 
                                    user['user_id'], 
                                    user['username'],
                                    execution_results=f"Execution failed: {execution_result.get('error')}"
                                )
                                st.rerun()
                        
                        except Exception as e:
                            st.error(f"Error executing query: {e}")
                            
                            st.session_state.user_manager.approve_query(
                                approval['approval_id'], 
                                user['user_id'], 
                                user['username'],
                                execution_results=f"Error: {str(e)}"
                            )
            
            with col2:
                if st.button("❌ Reject", key=f"reject_{approval['approval_id']}", use_container_width=True):
                    success = st.session_state.user_manager.reject_query(
                        approval['approval_id'], user['user_id'], user['username']
                    )
                    
                    if success:
                        st.success("Request rejected")
                        time.sleep(1)
                        st.rerun()
            
            st.markdown("---")

# ============================================================
# MY REQUESTS PAGE
# ============================================================

# ============================================================
# MY REQUESTS PAGE - WITH EXECUTE FOR APPROVED QUERIES
# ============================================================

# ============================================================
# MY REQUESTS PAGE - WITH EXECUTE FOR APPROVED QUERIES
# ============================================================

def my_requests_page():
    """Page for users to view their request history and execute approved queries"""
    
    user = st.session_state.user
    
    st.markdown("### My Requests")
    st.markdown(f"**{user['full_name']}** | {user['user_tier'].title()}")
    st.markdown("---")
    
    # Initialize agents if needed (for executing approved queries)
    if 'agents_initialized' not in st.session_state:
        with st.spinner("Initializing agents..."):
            try:
                st.session_state.agent1 = QueryPlannerAgent()
                st.session_state.agent2 = SQLGeneratorWithRBAC(user_tier=user['user_tier'])
                st.session_state.agent3 = ValidatorAgent(user_tier=user['user_tier'])
                st.session_state.agent4 = ExecutorAnalyzerClean()
                st.session_state.agents_initialized = True
            except Exception as e:
                st.error(f"Failed to initialize agents: {e}")
    
    # Back button
    if st.button("← Back to Dashboard"):
        st.session_state.current_page = 'query'
        st.rerun()
    
    # Refresh button
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    # Get user's requests
    requests_list = st.session_state.user_manager.get_my_requests(user.get('user_id', user['username']))
    
    if not requests_list:
        st.info("You haven't submitted any requests yet.")
        st.caption("When you submit queries that require approval, they will appear here.")
        return
    
    # Summary stats
    pending_count = sum(1 for r in requests_list if r['status'] == 'pending')
    approved_count = sum(1 for r in requests_list if r['status'] == 'approved')
    rejected_count = sum(1 for r in requests_list if r['status'] == 'rejected')
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total", len(requests_list))
    with col2:
        st.metric("Pending", pending_count)
    with col3:
        st.metric("Approved", approved_count)
    with col4:
        st.metric("Rejected", rejected_count)
    
    st.markdown("---")
    
    # Filter tabs
    tab1, tab2, tab3 = st.tabs(["🟡 Pending", "🟢 Approved", "📋 All"])
    
    with tab1:
        pending = [r for r in requests_list if r['status'] == 'pending']
        if pending:
            st.markdown(f"### Pending Approval: {len(pending)}")
            st.caption("These queries are waiting for manager/executive approval.")
            for req in pending:
                display_request_card_enhanced(req, user)
        else:
            st.info("No pending requests")
    
    with tab2:
        approved = [r for r in requests_list if r['status'] == 'approved']
        if approved:
            st.markdown(f"### Approved Requests: {len(approved)}")
            st.caption("Click 'Execute & View Results' to run your approved query.")
            for req in approved:
                display_request_card_enhanced(req, user)
        else:
            st.info("No approved requests yet")
    
    with tab3:
        st.markdown(f"### All Requests: {len(requests_list)}")
        for req in requests_list:
            display_request_card_enhanced(req, user, "all")


def display_request_card_enhanced(request: Dict, user: Dict, key_suffix: str = "default"):
    """Display a request card with execute functionality for approved queries"""
    
    status = request['status']
    status_icons = {
        'pending': '🟡',
        'approved': '🟢',
        'rejected': '🔴',
        'executed': '✅'
    }
    
    with st.container():
        # Header row
        col_icon, col_title, col_status = st.columns([0.5, 3, 1])
        
        with col_icon:
            st.markdown(f"### {status_icons.get(status, '⚪')}")
        
        with col_title:
            st.markdown(f"### Request #{request['approval_id']}")
            st.caption(f"Submitted: {request['created_at']}")
        
        with col_status:
            if status == 'pending':
                st.warning("PENDING")
            elif status == 'approved':
                st.success("APPROVED")
            elif status == 'rejected':
                st.error("REJECTED")
            else:
                st.info(status.upper())
        
        # Query and metrics
        st.markdown(f"**Query:** {request['query_text']}")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            cost = request.get('estimated_cost', 0) or 0
            st.metric("Est. Cost", f"${cost:.2f}")
        with col2:
            rows = request.get('estimated_rows', 0) or 0
            st.metric("Est. Rows", f"{rows:,}")
        with col3:
            risk = request.get('risk_level', 'unknown') or 'unknown'
            st.metric("Risk", risk.upper())
        
        # Status-specific content
        if status == 'pending':
            st.info("⏳ Waiting for manager/executive approval...")
        
        elif status == 'approved':
            st.success(f"✅ Approved by: {request.get('approver_username', 'Unknown')} on {request.get('reviewed_at', 'N/A')}")
            
            # Show approval notes if any
            if request.get('approval_notes'):
                with st.expander("📋 Approval Notes"):
                    st.info(request['approval_notes'])
            
            # EXECUTE BUTTON - Key feature!
            st.markdown("---")
            
            col_exec, col_space = st.columns([2, 3])
            
            with col_exec:
                # Check if we have cached results for this request
                cache_key = f"results_{request['approval_id']}"
                has_cached = cache_key in st.session_state
                
                button_label = "🔄 Re-run Query" if has_cached else "🚀 Execute & View Results"
                
                if st.button(
                    button_label, 
                    key=f"exec_approved_{request['approval_id']}_{key_suffix}", 
                    type="primary",
                # Replaced 'width="stretch"' with the correct Streamlit argument:
                    use_container_width=True
                ):
                    execute_approved_query(request, user)
                    st.rerun()  # Rerun to show results cleanly
            
            # Display cached results if available
            cache_key = f"results_{request['approval_id']}"
            if cache_key in st.session_state:
                display_cached_results(st.session_state[cache_key], request['approval_id'], key_suffix)
        
        elif status == 'rejected':
            st.error(f" Rejected by: {request.get('approver_username', 'Unknown')}")
            if request.get('approval_notes'):
                st.warning(f"Reason: {request['approval_notes']}")
        
        # SQL expander
        with st.expander("🔍 View SQL"):
            st.code(request.get('sql_generated', 'N/A'), language='sql')
        
        st.markdown("---")


def execute_approved_query(request: Dict, user: Dict):
    """Execute an approved query and cache results"""
    
    with st.spinner("Executing your approved query..."):
        try:
            sql_to_execute = request['sql_generated']
            
            # Add LIMIT if not present (safety measure)
            if 'LIMIT' not in sql_to_execute.upper():
                sql_to_execute = sql_to_execute.rstrip(';').strip()
                sql_to_execute += ' LIMIT 10000'
            
            # Execute
            execution_result = st.session_state.agent4.schema_tools.execute_snowflake_query(
                sql_to_execute, 
                limit=10000
            )
            
            if execution_result.get('success'):
                # Cache the results
                cache_key = f"results_{request['approval_id']}"
                st.session_state[cache_key] = execution_result
                # Results will be displayed after rerun
                
            else:
                st.error(f" Execution failed: {execution_result.get('error', 'Unknown error')}")
        
        except Exception as e:
            st.error(f" Error executing query: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())


def display_cached_results(execution_result: Dict, approval_id: int, key_suffix: str = "default"):
    """Display cached execution results"""

    
    if not execution_result.get('data'):
        st.info("No data returned")
        return
    
    df = pd.DataFrame(execution_result['data'])
    row_count = len(df)
    
    # Format numeric columns
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].round(3)
    
    # Results header
    st.markdown("###  Query Results")
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", f"{row_count:,}")
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        if row_count >= 10000:
            st.warning("Limited to 10K")
        else:
            st.success("Complete")
    
    # Warning if results were limited
    if row_count >= 10000:
        st.warning(" Showing first 10,000 rows. The full dataset may contain more records.")
    
    # Data table
    st.dataframe(df, use_container_width=True, height=400)
    
    # Download button with unique key
    csv = df.to_csv(index=False)
    
    st.download_button(
        label=" Download Results as CSV",
        data=csv,
        file_name=f"query_results_{approval_id}.csv",
        mime="text/csv",
        key=f"download_{key_suffix}_{approval_id}",
        use_container_width=False
    )


def execute_approved_query(request: Dict, user: Dict):
    """Execute an approved query and display results"""
    
    with st.spinner("Executing your approved query..."):
        try:
            sql_to_execute = request['sql_generated']
            
            # Add LIMIT if not present (safety measure)
            if 'LIMIT' not in sql_to_execute.upper():
                sql_to_execute = sql_to_execute.rstrip(';').strip()
                sql_to_execute += ' LIMIT 10000'
                st.info(" Added LIMIT 10000 for safety")
            
            # Execute
            execution_result = st.session_state.agent4.schema_tools.execute_snowflake_query(
                sql_to_execute, 
                limit=10000
            )
            
            if execution_result.get('success'):
                row_count = execution_result.get('row_count', 0)
                
                st.success(f" Query executed successfully! {row_count:,} rows returned")
                
                # Cache the results
                cache_key = f"results_{request['approval_id']}"
                st.session_state[cache_key] = execution_result
                
                # Display results immediately
                display_cached_results(execution_result, request['approval_id'])
                
            else:
                st.error(f"Execution failed: {execution_result.get('error', 'Unknown error')}")
        
        except Exception as e:
            st.error(f" Error executing query: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())



# ============================================================
# TAB 1: QUERY INTERFACE
# ============================================================

def render_tab1_query_agent():
    """Tab 1: Query Agent with HITL"""
    
    user = st.session_state.user
    
    # Access info
    access_info = {
        "analyst": " Access: Aggregated metrics, sentiment analysis |  Restricted: VIN, detailed text",
        "manager": " Access: Detailed analysis, complaint descriptions |  Restricted: VIN",
        "executive": " Access: Full data including VIN |  No restrictions"
    }
    
    st.info(access_info[user['user_tier']])
    
    # Initialize agents
    if 'agents_initialized' not in st.session_state:
        with st.spinner("Initializing AI agents..."):
            try:
                st.session_state.agent1 = QueryPlannerAgent()
                st.session_state.agent2 = SQLGeneratorWithRBAC(user_tier=user['user_tier'])
                st.session_state.agent3 = ValidatorAgent(user_tier=user['user_tier'])
                st.session_state.agent4 = ExecutorAnalyzerClean()
                st.session_state.agents_initialized = True
                st.success(" Agents ready")
                time.sleep(0.5)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to initialize agents: {e}")
                return
    
    st.markdown("### Query Interface")
    
    # Example queries expandable section
    with st.expander(" Example Questions", expanded=False):
        examples = [
            "Which manufacturers have the worst customer sentiment?",
            "How many recalls does Ford have?",
            "Show me safety-critical complaints with high safety keyword counts",
            "Compare Tesla vs Honda sentiment scores",
            "What are the top 5 manufacturers by recall count?",
            "Which vehicles have the best fuel economy?",
            "Show me recall trends over the past 5 years"
        ]
        
        cols = st.columns(2)
        for i, example in enumerate(examples):
            with cols[i % 2]:
                if st.button(example, key=f"ex_{i}", use_container_width=True):
                    st.session_state.selected_query = example
                    st.rerun()
    
    # Query input
    default_query = st.session_state.get('selected_query', '')
    user_query = st.text_area(
        "Enter your question",
        value=default_query,
        placeholder="Which manufacturers have the worst customer sentiment?",
        height=100,
        key='query_input'
    )
    
    # Clear selected query after use
    if 'selected_query' in st.session_state:
        del st.session_state.selected_query
    
    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        analyze_btn = st.button("🔍 Analyze Query", type="primary", use_container_width=True)
    
    with col2:
        clear_btn = st.button("Clear Results", use_container_width=True)
    
    if clear_btn:
        if 'last_result' in st.session_state:
            del st.session_state.last_result
        st.rerun()
    
    # Process query
    if analyze_btn and user_query:
        if 'last_result' in st.session_state:
            del st.session_state.last_result
        
        result = process_query_complete(user_query, user)
        
        if result:
            st.session_state.last_result = result
            display_results_with_dashboard(result)
    
    # Show previous result
    if 'last_result' in st.session_state:
        display_results_with_dashboard(
    st.session_state.last_result, 
    st.session_state.user, 
    st.session_state.last_result.get('execution_time_seconds', 0)
)

# ============================================================
# TAB 2: NEWS
# ============================================================

def render_tab2_news():
    """Tab 2: News Portal with AI Dashboard Generation"""
    
    st.markdown("###  Latest Automotive News")
    st.markdown('<p class="sub-header">Click "View Analysis" on any article for AI-powered insights</p>', unsafe_allow_html=True)
    
    # Filters
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            search_mode = st.selectbox(
                "Search Mode", 
                ["By Manufacturer", "Breaking News"], 
                key="news_mode"
            )
        
        with col2:
            if search_mode == "By Manufacturer":
                manufacturer = st.selectbox(
                    "Manufacturer", 
                    ["Ford", "Tesla", "GM", "Toyota", "Honda", "BMW", "Mercedes"], 
                    key="news_mfr"
                )
            else:
                hours_back = st.selectbox(
                    "Time", 
                    [24, 48, 72], 
                    format_func=lambda x: f"Last {x} hours", 
                    key="news_hours"
                )
        
    
    # Search button
    if st.button("🔍 Search News", type="primary", key="search_news_btn"):
        with st.spinner("Searching..."):
            if search_mode == "By Manufacturer":
                data = call_mcp_tool("get_news_by_manufacturer", {
                    "manufacturer": manufacturer, 
                    "days_back": 7, 
                    "limit": 5
                })
            else:
                data = call_mcp_tool("get_breaking_news", {
                    "hours": hours_back, 
                    "limit": 5
                })
            
            if data.get("success"):
                articles = data.get("articles", [])
                if articles:
                    st.success(f" Found {len(articles)} articles")
                    st.session_state.last_news_results = articles
                    # Clear any previous dashboard
                    if 'current_article_dashboard' in st.session_state:
                        del st.session_state.current_article_dashboard
                else:
                    st.warning("No articles found")
            else:
                st.error(f"Error: {data.get('error', 'Unknown error')}")
    
    # Display results
    if 'last_news_results' in st.session_state:
        articles = st.session_state.last_news_results
        
        st.markdown("---")
        st.markdown(f"### Results ({len(articles)} articles)")
        
        # Export button
        if articles:
            df = pd.DataFrame(articles)
            csv = df.to_csv(index=False)
            st.download_button(
                " Export CSV",
                csv,
                f"news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv",
                key="news_export"
            )
        
        st.markdown("---")
        
        # Display each article
        for idx, article in enumerate(articles, 1):
            article_id = article.get('article_id')
            
            # Article header
            col_num, col_title, col_badge, col_btn = st.columns([0.5, 4, 1.5, 1.5])
            
            with col_num:
                st.markdown(f"**{idx}.**")
            
            with col_title:
                st.markdown(f"**{article['title']}**")
                pub_date = format_datetime(article.get('published_date', ''))
                source = article.get('source', 'Unknown')
                st.caption(f" {pub_date} |  {source}")
            
            with col_badge:
                sentiment = article.get('sentiment', 'neutral')
                if sentiment:
                    emoji, css_class = get_sentiment_display(sentiment)
                    st.markdown(f"<span class='{css_class}'>{emoji} {sentiment.title()}</span>", unsafe_allow_html=True)
            
            with col_btn:
                # View Analysis button - triggers LLM call
                if st.button(" View Analysis", key=f"analyze_article_{article_id}", use_container_width=True):
                    st.session_state.selected_article_id = article_id
                    st.session_state.selected_article_title = article['title']
            
            # Show dashboard if this article is selected
            if st.session_state.get('selected_article_id') == article_id:
                with st.container():
                    st.markdown("---")
                    
                    # Check if we already have the dashboard cached
                    cache_key = f"dashboard_{article_id}"
                    
                    if cache_key not in st.session_state:
                        # Call MCP to generate dashboard (Claude LLM call happens here!)
                        with st.spinner(" Generating AI analysis..."):
                            dashboard_response = call_mcp_tool(
                                "generate_article_dashboard", 
                                {"article_id": article_id}
                            )
                            
                            if dashboard_response.get('success'):
                                st.session_state[cache_key] = dashboard_response
                            else:
                                st.error(f"Failed to generate dashboard: {dashboard_response.get('error', 'Unknown error')}")
                                st.session_state[cache_key] = None
                    
                    # Display the dashboard
                    dashboard = st.session_state.get(cache_key)
                    
                    if dashboard and dashboard.get('success'):
                        st.markdown(f"####  AI Analysis: {article['title'][:60]}...")
                        
                        # Render 4-section dashboard
                        render_article_dashboard(dashboard)
                        
                        # Read full article button
                        st.markdown("---")
                        col1, col2 = st.columns([2, 4])
                        with col1:
                            if article.get('url'):
                                st.link_button(" Read Full Article", article['url'], use_container_width=True)
                        with col2:
                            if st.button(" Close Analysis", key=f"close_{article_id}"):
                                del st.session_state.selected_article_id
                                st.rerun()
                    
                    st.markdown("---")
            
            st.divider()

# ============================================================
# TAB 2: EVENTS
# ============================================================

def render_tab2_events():
    """Tab 2: Events Portal with AI Dashboard Generation"""
    
    st.markdown("###  Upcoming Events")
    st.markdown('<p class="sub-header">Click "View Analysis" on any event for AI-powered insights</p>', unsafe_allow_html=True)
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        days_ahead = st.selectbox(
            "Time Horizon", 
            [30, 90, 180, 365], 
            format_func=lambda x: f"Next {x} days", 
            key="event_days"
        )
    
    with col2:
        event_type = st.selectbox(
            "Type", 
            ["All Types", "Auto Show", "Conference", "Summit", "Trade Show"], 
            key="event_type"
        )
    
    
    # Search button
    if st.button(" Find Events", type="primary", key="search_events_btn"):
        with st.spinner("Searching..."):
            payload = {"days_ahead": days_ahead, "limit": 5}
            if event_type != "All Types":
                payload["event_type"] = event_type.lower()
            
            data = call_mcp_tool("search_upcoming_events", payload)
            
            if data.get("success"):
                events = data.get("events", [])
                if events:
                    st.success(f" Found {len(events)} events")
                    st.session_state.last_events_results = events
                    if 'selected_event_id' in st.session_state:
                        del st.session_state.selected_event_id
                else:
                    st.warning("No events found")
            else:
                st.error(f"Error: {data.get('error', 'Unknown error')}")
    
    # Display results
    if 'last_events_results' in st.session_state:
        events = st.session_state.last_events_results
        
        st.markdown("---")
        st.markdown(f"### Results ({len(events)} events)")
        st.markdown("---")
        
        for idx, event in enumerate(events, 1):
            event_id = event.get('event_id')
            
            # Event header
            col_num, col_title, col_date, col_btn = st.columns([0.5, 4, 2, 1.5])
            
            with col_num:
                st.markdown(f"**{idx}.**")
            
            with col_title:
                st.markdown(f"**{event['title']}**")
                location = event.get('location', 'TBD')
                event_type_str = event.get('event_type', 'Event')
                st.caption(f" {location} |  {event_type_str}")
            
            with col_date:
                date_start = format_date(event.get('date_start', ''))
                st.markdown(f" **{date_start}**")
            
            with col_btn:
                if st.button(" View Analysis", key=f"analyze_event_{event_id}", use_container_width=True):
                    st.session_state.selected_event_id = event_id
            
            # Show dashboard if this event is selected
            if st.session_state.get('selected_event_id') == event_id:
                with st.container():
                    st.markdown("---")
                    
                    cache_key = f"event_dashboard_{event_id}"
                    
                    if cache_key not in st.session_state:
                        with st.spinner(" Generating AI analysis..."):
                            dashboard_response = call_mcp_tool(
                                "generate_event_dashboard",
                                {"event_id": event_id}
                            )
                            
                            if dashboard_response.get('success'):
                                st.session_state[cache_key] = dashboard_response
                            else:
                                st.error(f"Failed: {dashboard_response.get('error', 'Unknown')}")
                                st.session_state[cache_key] = None
                    
                    dashboard = st.session_state.get(cache_key)
                    
                    if dashboard and dashboard.get('success'):
                        st.markdown(f"####  AI Analysis: {event['title'][:60]}...")
                        
                        render_event_dashboard(dashboard)
                        
                        st.markdown("---")
                        col1, col2 = st.columns([2, 4])
                        with col1:
                            if event.get('url'):
                                st.link_button(" Event Details", event['url'], use_container_width=True)
                        with col2:
                            if st.button(" Close Analysis", key=f"close_event_{event_id}"):
                                del st.session_state.selected_event_id
                                st.rerun()
                    
                    st.markdown("---")
            
            st.divider()

# ============================================================
# MAIN DASHBOARD
# ============================================================

def main_dashboard():
    """Main dashboard with navigation"""
    
    user = st.session_state.user
    
    # Header
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        st.markdown('<h1 class="main-header">AutoInsight</h1>', unsafe_allow_html=True)
        badge_class = f"badge-{user['user_tier']}"
        st.markdown(
            f"**{user['full_name']}** | <span class='access-badge {badge_class}'>{user['user_tier'].upper()}</span>",
            unsafe_allow_html=True
        )
    
    with col2:
        # Analyst: My Requests button
        if user['user_tier'] == 'analyst':
            my_requests = st.session_state.user_manager.get_my_requests(user.get('user_id', user['username']))
            pending_count = len([r for r in my_requests if r['status'] == 'pending'])
            
            label = f"My Requests ({pending_count})" if pending_count > 0 else "My Requests"
            
            if st.button(label, use_container_width=True):
                st.session_state.current_page = 'my_requests'
                st.rerun()
        
        # Manager/Executive: Approvals button
        elif user['user_tier'] in ['manager', 'executive']:
            pending = st.session_state.user_manager.get_pending_approvals(user['user_tier'])
            pending_count = len(pending)
            
            label = f"Approvals ({pending_count})" if pending_count > 0 else "Approvals"
            
            if st.button(label, use_container_width=True):
                st.session_state.current_page = 'approvals'
                st.rerun()
    
    with col3:
        if st.button("Sign Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.session_state.current_page = 'query'
            for key in ['agent1', 'agent2', 'agent3', 'agent4', 'agents_initialized','last_query', 'last_sql', 'last_result', 'last_approval', 'current_query']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    st.markdown("---")
    
    # Route to correct page
    if st.session_state.current_page == 'approvals':
        approval_queue_page()
    elif st.session_state.current_page == 'my_requests':
        my_requests_page()
    else:
        # Main tabs
        tab1, tab2_news, tab2_events = st.tabs([
            " Query Agent",
            " Latest News",
            " Upcoming Events"
        ])
        
        with tab1:
            render_tab1_query_agent()
        
        with tab2_news:
            render_tab2_news()
        
        with tab2_events:
            render_tab2_events()

# ============================================================
# MAIN ROUTER
# ============================================================

def main():
    """Main application router"""
    
    if st.session_state.get('show_signup', False):
        signup_page()
    elif not st.session_state.logged_in:
        login_page()
    else:
        main_dashboard()


if __name__ == "__main__":
    main()