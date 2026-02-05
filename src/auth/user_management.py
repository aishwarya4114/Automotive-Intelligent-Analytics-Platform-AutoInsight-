"""
User authentication and management for AutoInsight
Production version - Snowflake backend
"""

import hashlib
import secrets
from typing import Optional, Dict, List
from datetime import datetime
import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv

load_dotenv()


class UserManager:
    """Manage user authentication and roles"""
    
    def __init__(
        self,
        account: str = None,
        user: str = None,
        password: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        role: str = None
    ):
        """Initialize Snowflake connection - loads from env vars if not provided"""
        
        self.connection_params = {
            "account": account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": user or os.getenv("SNOWFLAKE_USER"),
            "password": password or os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": database or os.getenv("SNOWFLAKE_DATABASE", "AUTOMOTIVE_AI"),
            "schema": schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "role": role or os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        }
        
        required_params = ["account", "user", "password"]
        missing_params = [p for p in required_params if not self.connection_params.get(p)]
        
        if missing_params:
            raise ValueError(
                f"Missing required Snowflake parameters: {', '.join(missing_params)}. "
                "Please set environment variables: "
                "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
            )
        
        self.schema = self.connection_params["schema"]
        self._create_tables()
    
    def _get_connection(self):
        """Create a new Snowflake connection"""
        try:
            return snowflake.connector.connect(**self.connection_params)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")
    
    def _create_tables(self):
        """Create user tables if they don't exist"""
        conn = None
        cursor = None
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS USERS (
                    USER_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
                    USERNAME VARCHAR(255) NOT NULL,
                    EMAIL VARCHAR(255) NOT NULL,
                    PASSWORD_HASH VARCHAR(64) NOT NULL,
                    SALT VARCHAR(32) NOT NULL,
                    USER_TIER VARCHAR(20) NOT NULL,
                    FULL_NAME VARCHAR(255),
                    ORGANIZATION VARCHAR(255),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    LAST_LOGIN TIMESTAMP_NTZ,
                    IS_ACTIVE BOOLEAN DEFAULT TRUE,
                    CONSTRAINT PK_USERS PRIMARY KEY (USER_ID),
                    CONSTRAINT UQ_USERNAME UNIQUE (USERNAME),
                    CONSTRAINT UQ_EMAIL UNIQUE (EMAIL)
                )
            """)
            
            # Sessions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS SESSIONS (
                    SESSION_ID VARCHAR(255) NOT NULL,
                    USER_ID NUMBER NOT NULL,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    EXPIRES_AT TIMESTAMP_NTZ,
                    CONSTRAINT PK_SESSIONS PRIMARY KEY (SESSION_ID),
                    CONSTRAINT FK_SESSIONS_USER FOREIGN KEY (USER_ID) REFERENCES USERS(USER_ID)
                )
            """)
            
            # Query audit log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS QUERY_AUDIT (
                    AUDIT_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
                    USER_ID NUMBER NOT NULL,
                    USERNAME VARCHAR(255),
                    USER_TIER VARCHAR(20),
                    QUERY_TEXT TEXT,
                    SQL_GENERATED TEXT,
                    TABLES_ACCESSED VARCHAR(1000),
                    ROW_COUNT NUMBER,
                    EXECUTION_TIME FLOAT,
                    SUCCESS BOOLEAN,
                    ERROR_MESSAGE TEXT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    CONSTRAINT PK_QUERY_AUDIT PRIMARY KEY (AUDIT_ID),
                    CONSTRAINT FK_QUERY_AUDIT_USER FOREIGN KEY (USER_ID) REFERENCES USERS(USER_ID)
                )
            """)
            
            # Approval queue table (NEW)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS APPROVAL_QUEUE (
                    APPROVAL_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
                    REQUESTER_USER_ID NUMBER NOT NULL,
                    REQUESTER_USERNAME VARCHAR(255),
                    REQUESTER_TIER VARCHAR(20),
                    QUERY_TEXT TEXT,
                    SQL_GENERATED TEXT,
                    ESTIMATED_COST FLOAT,
                    ESTIMATED_ROWS NUMBER,
                    RISK_LEVEL VARCHAR(20),
                    HITL_REASON TEXT,
                    STATUS VARCHAR(20) DEFAULT 'pending',
                    APPROVER_USER_ID NUMBER,
                    APPROVER_USERNAME VARCHAR(255),
                    APPROVAL_NOTES TEXT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    REVIEWED_AT TIMESTAMP_NTZ,
                    CONSTRAINT PK_APPROVAL_QUEUE PRIMARY KEY (APPROVAL_ID),
                    CONSTRAINT FK_APPROVAL_REQUESTER FOREIGN KEY (REQUESTER_USER_ID) REFERENCES USERS(USER_ID)
                )
            """)
            
            conn.commit()
            
        except snowflake.connector.errors.ProgrammingError as e:
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                raise Exception(
                    f"Database or Schema does not exist. Please verify:\n"
                    f"  - Database: {self.connection_params['database']}\n"
                    f"  - Schema: {self.schema}\n"
                    f"Original error: {error_msg}"
                )
            elif "insufficient privileges" in error_msg.lower():
                raise Exception(
                    f"Insufficient privileges to create tables.\n"
                    f"User '{self.connection_params['user']}' needs CREATE TABLE privilege.\n"
                    f"Original error: {error_msg}"
                )
            else:
                raise Exception(f"Snowflake SQL Error: {error_msg}")
        
        except snowflake.connector.errors.DatabaseError as e:
            raise Exception(f"Database Error: {str(e)}")
        
        except Exception as e:
            raise Exception(f"Failed to create tables: {str(e)}")
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def _hash_password(self, password: str, salt: str) -> str:
        """Hash password with salt"""
        return hashlib.sha256((password + salt).encode()).hexdigest()
    
    def create_user(
        self,
        username: str,
        email: str,
        password: str,
        user_tier: str,
        full_name: str = "",
        organization: str = ""
    ) -> Dict:
        """Create new user account"""
        
        if user_tier not in ['analyst', 'manager', 'executive']:
            return {
                "success": False,
                "error": "Invalid user tier. Must be: analyst, manager, or executive"
            }
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            salt = secrets.token_hex(16)
            password_hash = self._hash_password(password, salt)
            
            cursor.execute("""
                INSERT INTO USERS (USERNAME, EMAIL, PASSWORD_HASH, SALT, USER_TIER, FULL_NAME, ORGANIZATION)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (username, email, password_hash, salt, user_tier, full_name, organization))
            
            cursor.execute("SELECT USER_ID FROM USERS WHERE USERNAME = %s", (username,))
            result = cursor.fetchone()
            user_id = result[0] if result else None
            
            conn.commit()
            
            return {
                "success": True,
                "user_id": user_id,
                "username": username,
                "user_tier": user_tier,
                "message": f"User created successfully as {user_tier}"
            }
            
        except snowflake.connector.errors.IntegrityError as e:
            error_msg = str(e).lower()
            
            try:
                cursor.execute("SELECT COUNT(*) FROM USERS WHERE USERNAME = %s", (username,))
                if cursor.fetchone()[0] > 0:
                    return {"success": False, "error": "Username already exists"}
                
                cursor.execute("SELECT COUNT(*) FROM USERS WHERE EMAIL = %s", (email,))
                if cursor.fetchone()[0] > 0:
                    return {"success": False, "error": "Email already exists"}
            except:
                pass
            
            return {"success": False, "error": f"Database error: {str(e)}"}
        
        except Exception as e:
            return {"success": False, "error": f"Database error: {str(e)}"}
        
        finally:
            cursor.close()
            conn.close()
    
    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user and return user info"""
        
        conn = self._get_connection()
        cursor = conn.cursor(DictCursor)
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                SELECT USER_ID, USERNAME, EMAIL, PASSWORD_HASH, SALT, USER_TIER, 
                       FULL_NAME, ORGANIZATION, IS_ACTIVE
                FROM USERS
                WHERE USERNAME = %s
            """, (username,))
            
            result = cursor.fetchone()
            
            if not result:
                return None
            
            if not result['IS_ACTIVE']:
                return None
            
            password_hash = self._hash_password(password, result['SALT'])
            
            if password_hash != result['PASSWORD_HASH']:
                return None
            
            cursor.execute("""
                UPDATE USERS 
                SET LAST_LOGIN = CURRENT_TIMESTAMP()
                WHERE USER_ID = %s
            """, (result['USER_ID'],))
            conn.commit()
            
            return {
                "user_id": result['USER_ID'],
                "username": result['USERNAME'],
                "email": result['EMAIL'],
                "user_tier": result['USER_TIER'],
                "full_name": result['FULL_NAME'] or result['USERNAME'],
                "organization": result['ORGANIZATION'] or "AutoInsight"
            }
            
        finally:
            cursor.close()
            conn.close()
    
    def get_user_count(self) -> int:
        """Get total number of users"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            cursor.execute("SELECT COUNT(*) FROM USERS")
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()
            conn.close()
    
    def log_query(
        self,
        user_id: int,
        username: str,
        user_tier: str,
        query_text: str,
        sql_generated: str,
        tables_accessed: list,
        row_count: int,
        execution_time: float,
        success: bool,
        error_message: str = None
    ):
        """Log query execution for audit trail"""
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                INSERT INTO QUERY_AUDIT 
                (USER_ID, USERNAME, USER_TIER, QUERY_TEXT, SQL_GENERATED, 
                 TABLES_ACCESSED, ROW_COUNT, EXECUTION_TIME, SUCCESS, ERROR_MESSAGE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id, username, user_tier, query_text, sql_generated,
                ','.join(tables_accessed) if tables_accessed else '',
                row_count, execution_time, success, error_message
            ))
            
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    # ============================================================
    # APPROVAL QUEUE METHODS
    # ============================================================
    
    def submit_for_approval(
        self,
        requester_user_id: int,
        requester_username: str,
        requester_tier: str,
        query_text: str,
        sql_generated: str,
        estimated_cost: float,
        estimated_rows: int,
        risk_level: str,
        hitl_reason: str
    ) -> int:
        """Submit query for HITL approval"""
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                INSERT INTO APPROVAL_QUEUE
                (REQUESTER_USER_ID, REQUESTER_USERNAME, REQUESTER_TIER,
                 QUERY_TEXT, SQL_GENERATED, ESTIMATED_COST, ESTIMATED_ROWS,
                 RISK_LEVEL, HITL_REASON, STATUS)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
            """, (
                requester_user_id, requester_username, requester_tier,
                query_text, sql_generated, estimated_cost, estimated_rows,
                risk_level, hitl_reason
            ))
            
            cursor.execute("""
                SELECT MAX(APPROVAL_ID) FROM APPROVAL_QUEUE 
                WHERE REQUESTER_USER_ID = %s
            """, (requester_user_id,))
            result = cursor.fetchone()
            approval_id = result[0] if result else None
            
            conn.commit()
            return approval_id
            
        finally:
            cursor.close()
            conn.close()
    
    def get_pending_approvals(self, approver_tier: str) -> List[Dict]:
        """Get pending approvals for a user tier"""
        
        conn = self._get_connection()
        cursor = conn.cursor(DictCursor)
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Manager can approve analyst queries
            # Executive can approve both analyst and manager queries
            if approver_tier == 'manager':
                tier_filter = "REQUESTER_TIER = 'analyst'"
            elif approver_tier == 'executive':
                tier_filter = "REQUESTER_TIER IN ('analyst', 'manager')"
            else:
                # Analysts can't approve anything
                return []
            
            cursor.execute(f"""
                SELECT 
                    APPROVAL_ID, REQUESTER_USERNAME, REQUESTER_TIER,
                    QUERY_TEXT, SQL_GENERATED, ESTIMATED_COST, ESTIMATED_ROWS,
                    RISK_LEVEL, HITL_REASON, CREATED_AT, STATUS
                FROM APPROVAL_QUEUE
                WHERE {tier_filter} AND STATUS = 'pending'
                ORDER BY CREATED_AT DESC
            """)
            
            results = cursor.fetchall()
            
            approvals = []
            for row in results:
                approvals.append({
                    "approval_id": row['APPROVAL_ID'],
                    "requester_username": row['REQUESTER_USERNAME'],
                    "requester_tier": row['REQUESTER_TIER'],
                    "query_text": row['QUERY_TEXT'],
                    "sql_generated": row['SQL_GENERATED'],
                    "estimated_cost": row['ESTIMATED_COST'],
                    "estimated_rows": row['ESTIMATED_ROWS'],
                    "risk_level": row['RISK_LEVEL'],
                    "hitl_reason": row['HITL_REASON'],
                    "created_at": str(row['CREATED_AT']) if row['CREATED_AT'] else None,
                    "status": row['STATUS']
                })
            
            return approvals
            
        except Exception as e:
            print(f"Error fetching pending approvals: {str(e)}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def approve_query(
        self,
        approval_id: int,
        approver_user_id: int,
        approver_username: str,
        notes: str = "",
        execution_results: str = None
    ) -> bool:
        """Approve a pending query"""
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                UPDATE APPROVAL_QUEUE
                SET STATUS = 'approved',
                    APPROVER_USER_ID = %s,
                    APPROVER_USERNAME = %s,
                    APPROVAL_NOTES = %s,
                    REVIEWED_AT = CURRENT_TIMESTAMP()
                WHERE APPROVAL_ID = %s AND STATUS = 'pending'
            """, (approver_user_id, approver_username, notes or execution_results, approval_id))
            
            conn.commit()
            success = cursor.rowcount > 0
            return success
            
        finally:
            cursor.close()
            conn.close()
    
    def reject_query(
        self,
        approval_id: int,
        approver_user_id: int,
        approver_username: str,
        notes: str = ""
    ) -> bool:
        """Reject a pending query"""
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                UPDATE APPROVAL_QUEUE
                SET STATUS = 'rejected',
                    APPROVER_USER_ID = %s,
                    APPROVER_USERNAME = %s,
                    APPROVAL_NOTES = %s,
                    REVIEWED_AT = CURRENT_TIMESTAMP()
                WHERE APPROVAL_ID = %s AND STATUS = 'pending'
            """, (approver_user_id, approver_username, notes, approval_id))
            
            conn.commit()
            success = cursor.rowcount > 0
            return success
            
        finally:
            cursor.close()
            conn.close()
    
    def get_approval_by_id(self, approval_id: int) -> Optional[Dict]:
        """Get specific approval request"""
        
        conn = self._get_connection()
        cursor = conn.cursor(DictCursor)
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.execute("""
                SELECT 
                    APPROVAL_ID, REQUESTER_USERNAME, REQUESTER_TIER,
                    QUERY_TEXT, SQL_GENERATED, ESTIMATED_COST, ESTIMATED_ROWS,
                    RISK_LEVEL, HITL_REASON, STATUS, APPROVER_USERNAME,
                    APPROVAL_NOTES, CREATED_AT, REVIEWED_AT
                FROM APPROVAL_QUEUE
                WHERE APPROVAL_ID = %s
            """, (approval_id,))
            
            result = cursor.fetchone()
            
            if result:
                return {
                    "approval_id": result['APPROVAL_ID'],
                    "requester_username": result['REQUESTER_USERNAME'],
                    "requester_tier": result['REQUESTER_TIER'],
                    "query_text": result['QUERY_TEXT'],
                    "sql_generated": result['SQL_GENERATED'],
                    "estimated_cost": result['ESTIMATED_COST'],
                    "estimated_rows": result['ESTIMATED_ROWS'],
                    "risk_level": result['RISK_LEVEL'],
                    "hitl_reason": result['HITL_REASON'],
                    "status": result['STATUS'],
                    "approver_username": result['APPROVER_USERNAME'],
                    "approval_notes": result['APPROVAL_NOTES'],
                    "created_at": str(result['CREATED_AT']) if result['CREATED_AT'] else None,
                    "reviewed_at": str(result['REVIEWED_AT']) if result['REVIEWED_AT'] else None
                }
            
            return None
            
        finally:
            cursor.close()
            conn.close()
    
    def get_my_requests(self, user_identifier) -> List[Dict]:
        """Get all requests submitted by this user"""
        
        conn = self._get_connection()
        cursor = conn.cursor(DictCursor)
        
        try:
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Check if user_identifier is user_id (int) or username (str)
            if isinstance(user_identifier, int):
                query = """
                    SELECT 
                        APPROVAL_ID, QUERY_TEXT, SQL_GENERATED, 
                        ESTIMATED_COST, ESTIMATED_ROWS, RISK_LEVEL,
                        STATUS, APPROVER_USERNAME, APPROVAL_NOTES,
                        CREATED_AT, REVIEWED_AT
                    FROM APPROVAL_QUEUE
                    WHERE REQUESTER_USER_ID = %s
                    ORDER BY CREATED_AT DESC
                    LIMIT 50
                """
                cursor.execute(query, (user_identifier,))
            else:
                query = """
                    SELECT 
                        APPROVAL_ID, QUERY_TEXT, SQL_GENERATED, 
                        ESTIMATED_COST, ESTIMATED_ROWS, RISK_LEVEL,
                        STATUS, APPROVER_USERNAME, APPROVAL_NOTES,
                        CREATED_AT, REVIEWED_AT
                    FROM APPROVAL_QUEUE
                    WHERE REQUESTER_USERNAME = %s
                    ORDER BY CREATED_AT DESC
                    LIMIT 50
                """
                cursor.execute(query, (user_identifier,))
            
            results = cursor.fetchall()
            
            requests = []
            for row in results:
                requests.append({
                    "approval_id": row['APPROVAL_ID'],
                    "query_text": row['QUERY_TEXT'],
                    "sql_generated": row['SQL_GENERATED'],
                    "estimated_cost": row['ESTIMATED_COST'],
                    "estimated_rows": row['ESTIMATED_ROWS'],
                    "risk_level": row['RISK_LEVEL'],
                    "status": row['STATUS'],
                    "approver_username": row['APPROVER_USERNAME'],
                    "approval_notes": row['APPROVAL_NOTES'],
                    "created_at": str(row['CREATED_AT']) if row['CREATED_AT'] else None,
                    "reviewed_at": str(row['REVIEWED_AT']) if row['REVIEWED_AT'] else None
                })
            
            return requests
            
        except Exception as e:
            print(f"Error fetching user requests: {str(e)}")
            return []
        finally:
            cursor.close()
            conn.close()


# ============================================================
# RBAC RULES DEFINITION
# ============================================================

class RBACRules:
    """Column-level access control rules"""
    
    # Tier 1: Analyst - Public data, aggregated metrics
    TIER_1_ALLOWED = {
        "FACT_COMPLAINTS": [
            "complaint_id", "make", "model", "year",
            "sentiment_score", "sentiment_category", "sentiment_confidence",
            "crash_flag", "fire_flag", "injured", "deaths",
            "severity_level", "has_incident",
            "safety_keyword_count", "negation_count", "sentiment_category"
        ],
        "FACT_RECALLS": [
            "recall_id", "make", "model", "year", "document_type", "has_year_data"
        ],
        "FACT_RATINGS": "*",
        "FACT_FUEL_ECONOMY": "*",
        "FACT_ECONOMIC_INDICATORS": "*",
        "FACT_INVESTIGATIONS": [
            "investigation_id", "make", "model", "year",
            "component_name", "component_category",
            "investigation_status", "has_recall"
        ]
    }
    
    # Tier 2: Manager - Detailed analysis
    TIER_2_ALLOWED = {
        "FACT_COMPLAINTS": [
            "component_description", "fail_date", "date_added",
            "mileage", "fuel_type", "drive_train",
            "complaint_description", "vehicle_speed"
        ],
        "FACT_RECALLS": [
            "recall_summary", "primary_document"
        ],
        "FACT_RATINGS": "*",
        "FACT_FUEL_ECONOMY": "*",
        "FACT_ECONOMIC_INDICATORS": "*",
        "FACT_INVESTIGATIONS": "*"
    }
    
    # Tier 3: Executive - Full access
    TIER_3_ALLOWED = {
        "FACT_COMPLAINTS": "*",
        "FACT_RECALLS": "*",
        "FACT_RATINGS": "*",
        "FACT_FUEL_ECONOMY": "*",
        "FACT_ECONOMIC_INDICATORS": "*",
        "FACT_INVESTIGATIONS": "*"
    }
    
    # Explicitly blocked columns
    TIER_1_BLOCKED = {
        "FACT_COMPLAINTS": ["VIN", "complaint_description", "component_description", "mileage", "vehicle_speed"],
        "FACT_RECALLS": ["recall_summary"]
    }
    
    TIER_2_BLOCKED = {
        "FACT_COMPLAINTS": ["VIN"]
    }
    
    @classmethod
    def get_allowed_columns(cls, user_tier: str, table_name: str) -> list:
        """Get allowed columns for user tier and table"""
        
        if '.' in table_name:
            table_name = table_name.split('.')[-1]
        table_name = table_name.upper()
        
        if user_tier == 'analyst':
            allowed = cls.TIER_1_ALLOWED.get(table_name, [])
        elif user_tier == 'manager':
            tier1 = cls.TIER_1_ALLOWED.get(table_name, [])
            tier2 = cls.TIER_2_ALLOWED.get(table_name, [])
            
            if tier1 == "*" or tier2 == "*":
                allowed = "*"
            else:
                allowed = list(set(tier1 + tier2))
        else:
            allowed = cls.TIER_3_ALLOWED.get(table_name, "*")
        
        if allowed == "*":
            return "*"
        
        return [col.upper() for col in allowed]
    
    @classmethod
    def get_blocked_columns(cls, user_tier: str, table_name: str) -> list:
        """Get explicitly blocked columns for tier"""
        
        if '.' in table_name:
            table_name = table_name.split('.')[-1]
        table_name = table_name.upper()
        
        if user_tier == 'analyst':
            return cls.TIER_1_BLOCKED.get(table_name, [])
        elif user_tier == 'manager':
            return cls.TIER_2_BLOCKED.get(table_name, [])
        else:
            return []
    
    @classmethod
    def check_column_access(cls, user_tier: str, table_name: str, column_name: str) -> bool:
        """Check if user can access specific column"""
        
        allowed = cls.get_allowed_columns(user_tier, table_name)
        
        if allowed == "*":
            blocked = cls.get_blocked_columns(user_tier, table_name)
            return column_name.upper() not in [b.upper() for b in blocked]
        
        return column_name.upper() in allowed


if __name__ == "__main__":
    print("User Management Module - Production Version (Snowflake)")
    print("No demo users. Use signup page to create users.")