import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
SO_API_KEY = os.getenv("SO_API_KEY")

LANGUAGES = ["python", "javascript", "typescript", "rust", "go", "java", "ruby", "cpp"]

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"
LOGS_PATH = "data/logs"