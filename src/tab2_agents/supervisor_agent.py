"""
AutoInsight News & Events Agent using LangGraph
"""

from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

from .mcp_tools import ALL_TOOLS
from .prompts import SYSTEM_PROMPT

# Load environment variables
load_dotenv()

class AutoInsightAgent:
    """
    AutoInsight News & Events Agent
    
    Uses Claude with MCP tools to answer automotive news and events queries.
    """
    
    def __init__(self, model: str = "claude-sonnet-4-20250514", verbose: bool = False):
        """
        Initialize the AutoInsight agent
        
        Args:
            model: Claude model to use (default: claude-sonnet-4-20250514)
            verbose: Whether to print detailed execution logs
        """
        self.model = model
        self.verbose = verbose
        
        # Validate API key
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment variables")
        
        # Initialize Claude
        self.llm = ChatAnthropic(
            model=self.model,
            anthropic_api_key=api_key,
            temperature=0,  # Deterministic responses
            max_tokens=4096
        )
        
        # Create ReAct agent with tools
        self.agent = create_react_agent(
            self.llm,
            ALL_TOOLS
        )
        
        if self.verbose:
            print(f"✓ AutoInsight Agent initialized with {len(ALL_TOOLS)} tools")
            print(f"✓ Using model: {self.model}")
    
    def query(self, user_message: str) -> str:
        """
        Process a user query and return a response
        
        Args:
            user_message: Natural language query from user
            
        Returns:
            Agent's response as a string
        """
        try:
            if self.verbose:
                print(f"\n{'='*60}")
                print(f"USER QUERY: {user_message}")
                print(f"{'='*60}\n")
            
            # Invoke agent with system prompt + user message
            result = self.agent.invoke({
                "messages": [
                    SystemMessage(content=SYSTEM_PROMPT),
                    HumanMessage(content=user_message)
                ]
            })
            
            # Extract final response
            messages = result.get("messages", [])
            if messages:
                final_message = messages[-1]
                response = final_message.content if hasattr(final_message, 'content') else str(final_message)
                
                if self.verbose:
                    print(f"\n{'='*60}")
                    print(f"AGENT RESPONSE:")
                    print(f"{'='*60}")
                    print(response)
                    print(f"{'='*60}\n")
                
                return response
            else:
                return "No response generated"
                
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            if self.verbose:
                print(f"❌ {error_msg}")
            return error_msg
    
    def chat(self, conversation_history: List[Dict[str, str]] = None) -> str:
        """
        Multi-turn conversation mode
        
        Args:
            conversation_history: List of {"role": "user"|"assistant", "content": "..."}
            
        Returns:
            Agent's response
        """
        if not conversation_history:
            conversation_history = []
        
        # Convert to LangChain messages, starting with system message
        messages = [SystemMessage(content=SYSTEM_PROMPT)]
        
        for turn in conversation_history:
            if turn["role"] == "user":
                messages.append(HumanMessage(content=turn["content"]))
            elif turn["role"] == "assistant":
                messages.append(AIMessage(content=turn["content"]))
        
        # Invoke agent
        result = self.agent.invoke({"messages": messages})
        
        # Extract response
        messages = result.get("messages", [])
        if messages:
            final_message = messages[-1]
            return final_message.content if hasattr(final_message, 'content') else str(final_message)
        return "No response generated"
    
    def stream_query(self, user_message: str):
        """
        Stream response for real-time display
        
        Args:
            user_message: User query
            
        Yields:
            Chunks of the response
        """
        result = self.agent.stream({
            "messages": [
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=user_message)
            ]
        })
        
        for chunk in result:
            if "messages" in chunk:
                for msg in chunk["messages"]:
                    if hasattr(msg, 'content') and msg.content:
                        yield msg.content


def create_agent(model: str = "claude-sonnet-4-20250514", verbose: bool = False) -> AutoInsightAgent:
    """
    Factory function to create AutoInsight agent
    
    Args:
        model: Claude model to use
        verbose: Whether to enable verbose logging
        
    Returns:
        Initialized AutoInsightAgent instance
    """
    return AutoInsightAgent(model=model, verbose=verbose)


# ============================================================================
# INTERACTIVE CLI MODE
# ============================================================================

def interactive_mode():
    """Run agent in interactive CLI mode"""
    print("="*60)
    print("AutoInsight News & Events Agent")
    print("="*60)
    print("\nType your queries below. Type 'exit' to quit.\n")
    
    agent = create_agent(verbose=True)
    
    while True:
        try:
            user_input = input("\n🔍 You: ").strip()
            
            if user_input.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if not user_input:
                continue
            
            print("\n🤖 Agent: ", end="", flush=True)
            response = agent.query(user_input)
            print(response)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    interactive_mode()