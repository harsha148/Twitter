import json
import time
from datetime import datetime
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class User:
    def __init__(self, user_data):
        self.user_id = user_data['id_str']
        self.name = user_data['name']
        self.screen_name = user_data['screen_name']
        self.followers_count = user_data['followers_count']
        self.friends_count = user_data['friends_count']
        self.tweets_count = user_data['statuses_count']
        self.verified = user_data['verified']
        self.created_at = self.get_created_date(user_data['created_at'])
        self.description = user_data.get('description', '')
        self.location = user_data.get('location', '')
        self.profile_image_url = user_data.get('profile_image_url', '')
        
    @staticmethod
    def get_created_date(created_at):
        created_at_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        created_at_date = created_at_date.strftime("%Y-%m-%d %H:%M:%S")
        return created_at_date
    
    def get_user_dict(self):
        return {
            'user_id': self.user_id,
            'name': self.name,
            'screen_name': self.screen_name,
            'followers_count': self.followers_count,
            'friends_count': self.friends_count,
            'tweets_count': self.tweets_count,
            'verified': self.verified,
            'created_at': self.created_at,
            'description': self.description,
            'location': self.location,
            'profile_image_url': self.profile_image_url
        }

def connect_to_supabase():
    """Connect to Supabase database"""
    try:
        supabase_url = 'https://krvusrtfpyfuxdpqtxww.supabase.co'
        supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtydnVzcnRmcHlmdXhkcHF0eHd3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM0MDUyMTYsImV4cCI6MjA2ODk4MTIxNn0.vOY8_ZY8SEZ5TIMihgnIfDL5jdEVvYhQM5ap_r8mrOg'
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        supabase: Client = create_client(supabase_url, supabase_key)
        print("Connected to Supabase database")
        return supabase
    except Exception as e:
        print(f"Error occurred while connecting to Supabase: {e}")
        return None

def create_users_table(supabase):
    """Create users table in Supabase"""
    try:
        # Create the users table using SQL
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            screen_name VARCHAR(255),
            followers_count INTEGER DEFAULT 0,
            friends_count INTEGER DEFAULT 0,
            tweets_count INTEGER DEFAULT 0,
            verified BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP,
            description TEXT,
            location VARCHAR(255),
            profile_image_url TEXT,
            created_at_utc TIMESTAMP DEFAULT NOW()
        );
        """
        
        # Execute the query using Supabase's rpc method
        result = supabase.rpc('execute_sql', {'sql': create_table_query}).execute()
        print("Users table created successfully")
        return True
    except Exception as e:
        print(f"Error creating users table: {e}")
        return False

def user_exists(supabase, user_id):
    """Check if user already exists in the database"""
    try:
        result = supabase.table('users').select('user_id').eq('user_id', user_id).execute()
        return len(result.data) > 0
    except Exception as e:
        print(f"Error checking if user exists: {e}")
        return False

def insert_user(supabase, user_data):
    """Insert a new user into the database"""
    try:
        result = supabase.table('users').insert(user_data).execute()
        return True
    except Exception as e:
        print(f"Error inserting user: {e}")
        return False

def update_user(supabase, user_id, update_data):
    """Update an existing user in the database"""
    try:
        result = supabase.table('users').update(update_data).eq('user_id', user_id).execute()
        return True
    except Exception as e:
        print(f"Error updating user: {e}")
        return False

def get_users_count(supabase):
    """Get total number of users in the database"""
    try:
        result = supabase.table('users').select('*', count='exact').execute()
        return result.count if result.count is not None else 0
    except Exception as e:
        print(f"Error getting users count: {e}")
        return 0

def load_user_data_to_supabase(supabase, file_path):
    """Load user data from JSON file to Supabase database"""
    start_time = time.time()
    processed_count = 0
    inserted_count = 0
    updated_count = 0
    
    try:
        with open(file_path, "r") as read_file:
            for line in read_file:
                try:
                    data = json.loads(line)
                    
                    # Extract user data from tweet
                    user_data = data.get('user')
                    if not user_data:
                        continue
                    
                    processed_count += 1
                    
                    # Create user object
                    user = User(user_data)
                    user_dict = user.get_user_dict()
                    
                    # Check if user already exists
                    if user_exists(supabase, user_dict['user_id']):
                        # Update existing user if needed
                        # You can add logic here to update user data if it has changed
                        updated_count += 1
                    else:
                        # Insert new user
                        if insert_user(supabase, user_dict):
                            inserted_count += 1
                    
                    # Print progress every 1000 users
                    if processed_count % 1000 == 0:
                        print(f"Processed {processed_count} users, inserted {inserted_count}, updated {updated_count}")
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line: {e}")
                    continue
        
        total_time = time.time() - start_time
        print(f"Successfully processed {processed_count} users")
        print(f"Inserted {inserted_count} new users")
        print(f"Updated {updated_count} existing users")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Total users in database: {get_users_count(supabase)}")
        
    except Exception as e:
        print(f"Error loading user data: {e}")

def create_indexes(supabase):
    """Create indexes for better query performance"""
    try:
        # Create index on user_id for faster lookups
        index_query = """
        CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_screen_name ON users(screen_name);
        CREATE INDEX IF NOT EXISTS idx_users_followers_count ON users(followers_count DESC);
        CREATE INDEX IF NOT EXISTS idx_users_verified ON users(verified);
        """
        
        result = supabase.rpc('execute_sql', {'sql': index_query}).execute()
        print("Indexes created successfully")
        return True
    except Exception as e:
        print(f"Error creating indexes: {e}")
        return False

def main():
    """Main function to run the user data processing"""
    print("Starting user data processing for Supabase...")
    
    # Connect to Supabase
    supabase = connect_to_supabase()
    if not supabase:
        print("Failed to connect to Supabase. Exiting.")
        return
    
    # # Create users table
    # if not create_users_table(supabase):
    #     print("Failed to create users table. Exiting.")
    #     return
    
    # Load user data from file
    # Update this path to your user data file
    file_path = "../data/corona-out-3"  # Update this path
    
    if os.path.exists(file_path):
        load_user_data_to_supabase(supabase, file_path)
        
        # Create indexes for better performance
        # create_indexes(supabase)
        
        print("User data processing completed successfully!")
    else:
        print(f"File not found: {file_path}")
        print("Please update the file_path variable to point to your user data file.")

if __name__ == "__main__":
    main() 