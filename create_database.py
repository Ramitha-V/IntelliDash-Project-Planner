import sqlite3
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# --- Configuration (Increased for a larger dataset) ---
DB_FILE = "project_data.db"
NUM_MEMBERS = 50      # Increased from 25
NUM_PROJECTS = 15     # Increased from 6
NUM_TASKS = 1200      # Increased from 250

def create_database_schema(conn):
    """Creates the database tables (schema is unchanged)."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS tasks")
    cursor.execute("DROP TABLE IF EXISTS projects")
    cursor.execute("DROP TABLE IF EXISTS team_members")

    cursor.execute("""
    CREATE TABLE team_members (
        member_id TEXT PRIMARY KEY, member_name TEXT NOT NULL, role TEXT, skills TEXT
    )""")
    cursor.execute("""
    CREATE TABLE projects (
        project_id TEXT PRIMARY KEY, project_name TEXT NOT NULL, project_manager_id TEXT,
        FOREIGN KEY (project_manager_id) REFERENCES team_members (member_id)
    )""")
    cursor.execute("""
    CREATE TABLE tasks (
        task_id TEXT PRIMARY KEY, project_id TEXT, task_title TEXT NOT NULL,
        assigned_to_id TEXT, dependencies TEXT, estimated_start_date TEXT,
        estimated_end_date TEXT, actual_start_date TEXT, actual_end_date TEXT,
        priority TEXT, status TEXT, risk TEXT,
        FOREIGN KEY (project_id) REFERENCES projects (project_id),
        FOREIGN KEY (assigned_to_id) REFERENCES team_members (member_id)
    )""")
    conn.commit()
    print("Database tables created successfully.")z

def generate_and_insert_data(conn):
    """Generates a larger, more varied synthetic dataset tailored for Dassault Systèmes."""
    fake = Faker()
    
    # --- Expanded Dassault Systèmes Specific Data Pools ---
    DS_ROLES = ["R&D Software Engineer", "QA Analyst", "Product Manager", "Software Architect", "Technical Consultant", "DevOps Engineer", "UI/UX Designer", "Kernel Developer", "Technical Fellow", "Release Manager", "Data Scientist"]
    DS_SKILLS = ["C++", "CATIA V6", "ENOVIA", "SIMULIA", "3DEXPERIENCE", "Cloud Architecture", "JavaScript", "Kernel Development", "QA Automation", "Database Optimization", "Geometric Modeling", "CUDA", "WebGL", "Microservices", "Python", "Machine Learning"]
    DS_PROJECT_PREFIXES = ["CATIA", "ENOVIA", "SOLIDWORKS", "SIMULIA", "DELMIA", "3DEXPERIENCE Platform", "BIOVIA", "GEOVIA"]
    DS_PROJECT_SUFFIXES = ["Kernel Upgrade", "Cloud Integration", "Performance Enhancement", "2026x Release", "Collaboration Module", "Data Analytics Service", "MBSE Framework", "Sustainability Dashboard", "Virtual Twin Connector", "AI Co-pilot"]
    TASK_VERBS = ["Implement", "Debug", "Optimize", "Refactor", "Develop", "Test", "Document", "Deploy", "Integrate", "Validate", "Enhance", "Research", "Prototype", "Review"]
    TASK_NOUNS = ["the new geometry kernel", "the user authentication service", "the rendering pipeline", "the data model synchronization", "the simulation solver", "the API for the CATIA module", "the build automation script", "the ENOVIA database schema", "the point cloud processing algorithm", "the new licensing component", "the UI for the results viewer", "the security vulnerability patch"]

    # --- 1. Generate Team Members ---
    members = []
    for i in range(1, NUM_MEMBERS + 1):
        members.append({
            "member_id": f"USER-{100 + i}",
            "member_name": fake.name(),
            "role": random.choice(DS_ROLES),
            "skills": ", ".join(random.sample(DS_SKILLS, k=random.randint(3, 6)))
        })
    members_df = pd.DataFrame(members)
    members_df.to_sql('team_members', conn, if_exists='append', index=False)
    print(f"Inserted {len(members_df)} relevant team members.")

    # --- 2. Generate Projects ---
    projects = []
    member_ids = members_df["member_id"].tolist()
    for i in range(1, NUM_PROJECTS + 1):
        projects.append({
            "project_id": f"PROJ-00{i}",
            "project_name": f"{random.choice(DS_PROJECT_PREFIXES)} {random.choice(DS_PROJECT_SUFFIXES)}",
            "project_manager_id": random.choice(member_ids)
        })
    projects_df = pd.DataFrame(projects)
    projects_df.to_sql('projects', conn, if_exists='append', index=False)
    print(f"Inserted {len(projects_df)} relevant projects.")

    # --- 3. Generate Tasks (Logic for dates/risk is the same) ---
    tasks = []
    project_ids = projects_df["project_id"].tolist()
    task_ids = [f"TASK-{5000 + i}" for i in range(1, NUM_TASKS + 1)]
    for i, task_id in enumerate(task_ids):
        est_start = datetime.now() - timedelta(days=random.randint(20, 365))
        est_duration = random.randint(3, 20)
        completed = random.random() > 0.4
        actual_end = None
        if completed:
            status = "Completed"
            actual_duration_delta = random.randint(-3, 10)
            actual_end = est_start + timedelta(days=est_duration + actual_duration_delta)
            risk = "Late" if actual_duration_delta > 4 else "On Track"
        else:
            status = random.choice(["In Progress", "To Do", "Blocked"])
            risk = "At Risk" if (datetime.now() > est_start + timedelta(days=est_duration)) else "On Track"
        dependencies = random.choice(task_ids[:i]) if i > 10 and random.random() > 0.6 else None

        tasks.append({
            "task_id": task_id, "project_id": random.choice(project_ids),
            "task_title": f"{random.choice(TASK_VERBS)} {random.choice(TASK_NOUNS)}",
            "assigned_to_id": random.choice(member_ids), "dependencies": dependencies,
            "estimated_start_date": (est_start).date().isoformat(),
            "estimated_end_date": (est_start + timedelta(days=est_duration)).date().isoformat(),
            "actual_start_date": (est_start + timedelta(days=random.randint(-1,2))).date().isoformat(),
            "actual_end_date": actual_end.date().isoformat() if actual_end else None,
            "priority": random.choice(["High", "Medium", "Low"]), "status": status, "risk": risk
        })
    tasks_df = pd.DataFrame(tasks)
    tasks_df.to_sql('tasks', conn, if_exists='append', index=False)
    print(f"Inserted {len(tasks_df)} relevant tasks.")

if __name__ == "__main__":
    connection = sqlite3.connect(DB_FILE)
    create_database_schema(connection)
    print("\n--- Generating Expanded Dassault Systèmes Tailored Data ---")
    generate_and_insert_data(connection)
    connection.close()
    print(f"\n Database '{DB_FILE}' created and populated with a large, relevant dataset!")

