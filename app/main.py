import os

from fastapi import FastAPI
from supabase import Client, create_client
from dotenv import load_dotenv

from fastapi import FastAPI
from models import User

# loading environment variables that store the supabase URL and API key
load_dotenv()

app = FastAPI()


api_url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_ADMIN")


def create_supabase_client():
    supabase: Client = create_client(api_url, key)
    return supabase


app = FastAPI()

# Initialize supabase client
supabase = create_supabase_client()


def user_exists(key: str = "email", value: str = None):
    user = supabase.from_("users").select("*").eq(key, value).execute()
    return len(user.data) > 0


# Create a new user
@app.post("/user")
def create_user(user: User):
    try:
        # Convert email to lowercase
        user_email = user.email.lower()

        # Check if user already exists
        if user_exists(value=user_email):
            return {"message": "User already exists"}

        # Add user to users table
        user = (
            supabase.from_("users")
            .insert({"name": user.name, "email": user_email})
            .execute()
        )

        # Check if user was added
        if user:
            return {"message": "User created successfully"}
        else:
            return {"message": "User creation failed"}
    except Exception as e:
        print("Error: ", e)
        return {"message": "User creation failed"}
