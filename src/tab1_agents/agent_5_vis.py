import pandas as pd
import streamlit as st
import plotly.express as px
from typing import Dict, List, Optional

# --- PROFESSIONAL AESTHETICS ---
# Single color for charts without segmentation (e.g., Bar Chart ranking)
PROFESSIONAL_COLOR = '#4C78A8' # A clean, professional blue
# Subtle discrete colors for charts with segmentation (e.g., Scatter/Box plot categories)
PROFESSIONAL_DISCRETE_COLORS = px.colors.qualitative.G10 
# Clean template for professional look
PROFESSIONAL_TEMPLATE = 'plotly_white'
DARK_TEXT_COLOR = '#333333'
# -------------------------------

def get_column_types(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Classifies DataFrame columns into generic types (Categorical, Quantitative, Time)
    to enable dynamic visualization.
    """
    
    col_map = {'Cat': [], 'Quant': [], 'Time': []}
    
    # 1. Standardize and simplify column names for consistency
    df.columns = [col.upper().replace('.', '_') for col in df.columns]
    
    for col in df.columns:
        # Skip columns that are clearly ID's or uninformative
        if 'ID' in col or df[col].isnull().all():
            continue

        # Check for TIME related columns
        if 'YEAR' in col or df[col].dtype in ['datetime64', 'timedelta64']:
            col_map['Time'].append(col)
        
        # Check for Quantitative columns
        elif df[col].dtype in ['int64', 'float64']:
            # Treat low-cardinality numbers (like safety ratings 1-5) as categorical if the dataset is large
            if len(df[col].unique()) <= 10 and len(df) > 50:
                 col_map['Cat'].append(col)
            else:
                col_map['Quant'].append(col)
        
        # Everything else (objects/strings) is Categorical
        else:
            col_map['Cat'].append(col)

    return col_map

def render_analyst_visualizations(df_results: pd.DataFrame, query_plan: Dict):
    """
    Visualization Agent: Selects and renders the SINGLE BEST industry-standard chart
    based on the dynamic column analysis and query intent, applying a professional theme.
    """
    
    if df_results.empty:
        st.error("No data returned to generate visualizations.")
        return

    df = df_results.copy()
    col_types = get_column_types(df)
    
    # Standardize column names based on the analysis done in get_column_types
    df.columns = [col.upper().replace('.', '_') for col in df.columns]

    # --- Dynamic Column Assignment based on priority ---
    primary_quant = col_types['Quant'][0] if col_types['Quant'] else None
    
    # Get the categorical column with the most unique values (best for ranking/identification)
    primary_cat = sorted(col_types['Cat'], key=lambda x: df[x].nunique(), reverse=True)[0] if col_types['Cat'] else None
    
    secondary_cat = col_types['Cat'][1] if len(col_types['Cat']) > 1 else None
    secondary_quant = col_types['Quant'][1] if len(col_types['Quant']) > 1 else None
    
    st.markdown("---")
    st.title("📈 Primary Visualization")
    
    # --- CHART SELECTION AND RENDERING ---
    
    rendered = False
    
    # 1. PRIORITY 1: RANKING/COMPARISON (Horizontal Bar Chart)
    if primary_cat and primary_quant and 5 <= df[primary_cat].nunique() <= 20:
        st.header(f"1. Comparison: {primary_cat.replace('_', ' ').title()} Ranking by {primary_quant.replace('_', ' ').title()}")
        st.markdown("<sub>Horizontal Bar Chart: Best for comparing performance of a few entities.</sub>", unsafe_allow_html=True)
        
        df_sorted = df.sort_values(by=primary_quant, ascending=True)
        
        fig_rank = px.bar(
            df_sorted,
            x=primary_quant,
            y=primary_cat, 
            orientation='h',
            # NOTE: Removed 'color=primary_cat' to enforce single gradient color
            title=f'{primary_quant.replace("_", " ").title()} by {primary_cat.replace("_", " ").title()}',
            color_discrete_sequence=[PROFESSIONAL_COLOR], # Single professional color
            template=PROFESSIONAL_TEMPLATE,
            height=400 
        )
        fig_rank.update_layout(
            font_color=DARK_TEXT_COLOR,
            xaxis=dict(
                tickfont=dict(color=DARK_TEXT_COLOR), 
                linecolor=DARK_TEXT_COLOR, 
                gridcolor='rgba(150, 150, 150, 0.2)'
            ),
            # CRITICAL FIX: Merged categoryorder with dark text properties
            yaxis=dict(
                categoryorder='total ascending',
                tickfont=dict(color=DARK_TEXT_COLOR), 
                linecolor=DARK_TEXT_COLOR
            )
        ) 
        st.plotly_chart(fig_rank, use_container_width=True)
        rendered = True

    # 2. PRIORITY 2: RELATIONSHIP/TRADE-OFF (Scatter Plot)
    if not rendered and primary_quant and secondary_quant and primary_cat:
        st.header(f"1. Relationship: {primary_quant.replace('_', ' ').title()} vs {secondary_quant.replace('_', ' ').title()}")
        st.markdown("<sub>Scatter Plot: Visualizes trade-offs and identifies outliers segmented by category.</sub>", unsafe_allow_html=True)
        
        fig_scatter = px.scatter(
            df, 
            x=primary_quant, 
            y=secondary_quant, 
            #color=primary_cat,
            #title=f'{primary_quant.replace("_", " ").title()} by {primary_cat.replace("_", " ").title()}',
            hover_data=[primary_cat, secondary_cat] if secondary_cat else [primary_cat],
            color_discrete_sequence=PROFESSIONAL_DISCRETE_COLORS, # Subtle discrete palette for segments
            template=PROFESSIONAL_TEMPLATE,
            title=f'Correlation Analysis'
        )
        fig_scatter.update_layout(
            font_color=DARK_TEXT_COLOR,
            xaxis=dict(tickfont=dict(color=DARK_TEXT_COLOR), linecolor=DARK_TEXT_COLOR, gridcolor='rgba(150, 150, 150, 0.2)'),
            yaxis=dict(tickfont=dict(color=DARK_TEXT_COLOR), linecolor=DARK_TEXT_COLOR, gridcolor='rgba(150, 150, 150, 0.2)')
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        rendered = True
        
    # 3. PRIORITY 3: DISTRIBUTION/SEGMENTATION (Box Plot)
    if not rendered and primary_quant and secondary_cat:
        st.header(f"1. Distribution of {primary_quant.replace('_', ' ').title()} by {secondary_cat.replace('_', ' ').title()}")
        st.markdown("<sub>Box Plot: Compares median, spread, and identifies outliers across different segments.</sub>", unsafe_allow_html=True)
        
        fig_box = px.box(
            df, 
            x=secondary_cat, 
            y=primary_quant, 
            color=secondary_cat,
            points="all", 
            color_discrete_sequence=PROFESSIONAL_DISCRETE_COLORS, # Subtle discrete palette for segments
            template=PROFESSIONAL_TEMPLATE,
            title=f'{primary_quant.replace("_", " ").title()} Distribution by {secondary_cat.replace("_", " ").title()}'
        )
        fig_box.update_layout(
            showlegend=False,
            font_color=DARK_TEXT_COLOR,
            xaxis=dict(tickfont=dict(color=DARK_TEXT_COLOR), linecolor=DARK_TEXT_COLOR, gridcolor='rgba(150, 150, 150, 0.2)'),
            yaxis=dict(tickfont=dict(color=DARK_TEXT_COLOR), linecolor=DARK_TEXT_COLOR, gridcolor='rgba(150, 150, 150, 0.2)')
        )
        
        st.plotly_chart(fig_box, use_container_width=True)
        rendered = True
    
    # 4. FALLBACK: Simple Data Table
    if not rendered:
        st.warning("Could not automatically determine the best chart type. Displaying raw data.")
        st.dataframe(df)