import os
import time
import subprocess
import argparse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import json

# Define constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
DEFAULT_SHEET_RANGE = 'B2:B'  # Username column
OUTPUT_RANGE_START = 'G'      # Start writing data from column G
OUTPUT_RANGE_END = 'M'       # End at column M

# Add these constants at the top with other constants
RATE_LIMIT_DELAY = 30  # Delay between profile requests in seconds
BATCH_SIZE = 10        # Number of requests before taking a longer break
BATCH_DELAY = 300     # Delay after each batch (5 minutes)

def get_google_sheets_service():
    creds = None
    # Get credentials from environment variables
    client_id = os.getenv('GOOGLE_CLIENT_ID')
    client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
    refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')
    
    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Missing Google credentials environment variables")
    
    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret
    )
    
    if not creds.valid:
        if creds.expired:
            creds.refresh(Request())
    
    return build('sheets', 'v4', credentials=creds)

def get_instagram_data(username, session_id):
    if not username:
        print("Skipping empty username")
        return None
        
    try:
        print(f"Fetching data for {username}...")
        result = subprocess.run(
            ['toutatis', '-u', username.strip(), '-s', session_id], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error fetching data for {username}: {result.stderr}")
            return None
        
        # Parse the output with combined fields
        data = {}
        for line in result.stdout.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # Handle combined fields with " | "
                if ' | ' in value:
                    parts = value.split(' | ')
                    for part in parts:
                        if ':' in part:
                            sub_key, sub_value = part.strip().split(':', 1)
                            data[sub_key.strip()] = sub_value.strip()
                        else:
                            data[key] = part.strip()
                else:
                    data[key] = value

        return data
        
    except Exception as e:
        print(f"Exception processing {username}: {str(e)}")
        return None

def update_sheet(spreadsheet_id, session_id, test_mode=False, force_update=False):
    service = get_google_sheets_service()
    sheet = service.spreadsheets()
    
    try:
        # Get both usernames and existing data
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f'B2:{OUTPUT_RANGE_END}100'  # Get usernames and existing data
        ).execute()
        
        rows = result.get('values', [])
        if not rows:
            print("No usernames found in spreadsheet")
            return
            
        if test_mode:
            print("TEST MODE: Processing only first row")
            rows = rows[:1]
        
        for idx, row in enumerate(rows, start=2):
            try:
                # Add batch delay every BATCH_SIZE requests
                if (idx - 2) % BATCH_SIZE == 0 and idx > 2:
                    print(f"📋 Taking a {BATCH_DELAY}s break to avoid rate limits...")
                    time.sleep(BATCH_DELAY)

                username = row[0].strip()
                
                # Check if row already has data
                has_data = len(row) > 5 and any(row[5:])  # Check if any data exists after column G
                
                if has_data and not force_update:
                    print(f"⏭️  Skipping {username} - already processed (use --force to update)")
                    continue
                
                print(f"⏳ Waiting {RATE_LIMIT_DELAY}s before next request...")
                time.sleep(RATE_LIMIT_DELAY)
                
                data = get_instagram_data(username, session_id)
                
                if data:
                    # Map the fields according to Toutatis output format
                    values = [[
                        data.get('Follower', ''),
                        data.get('Following', ''),
                        data.get('Number of posts', ''),
                        data.get('Biography', ''),
                        data.get('Full Name', ''),
                        data.get('Verified', ''),
                        data.get('Is private Account', ''),
                        data.get('Is buisness Account', ''),  # Added business account status
                        data.get('userID', ''),               # Added user ID
                        data.get('IGTV posts', ''),          # Added IGTV posts
                        data.get('Linked WhatsApp', ''),     # Added WhatsApp status
                        data.get('Memorial Account', ''),    # Added memorial account status
                        data.get('New Instagram user', '')   # Added new user status
                    ]]
                    
                    # Update range to include new columns
                    range_name = f'{OUTPUT_RANGE_START}{idx}:S{idx}'  # Extended to column S
                    sheet.values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body={'values': values}
                    ).execute()
                    print(f"✓ Updated data for {username}")
                    
                    # Respect rate limits
                    time.sleep(2)
                    
            except Exception as e:
                print(f"Error processing row {idx}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Failed to process spreadsheet: {str(e)}")

def get_session_id():
    """Get Instagram session ID from environment variable."""
    session_id = os.getenv('INSTAGRAM_SESSION_ID')
    if not session_id:
        raise ValueError("INSTAGRAM_SESSION_ID environment variable is not set")
    return session_id

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Instagram Data Enrichment Tool')
    parser.add_argument('--sheet', '-s', help='Google Sheet ID', 
                       default='1sZLn15IQJmPotCAEMLN66tpOjIef-pT618MLHllIP0k')
    parser.add_argument('--session', '-i', help='Instagram session ID',
                       default=get_session_id())
    parser.add_argument('--test', '-t', action='store_true', help='Test mode - process only first row')
    parser.add_argument('--force', '-f', action='store_true', 
                       help='Force update of already processed rows')
    
    args = parser.parse_args()
    update_sheet(args.sheet, args.session, args.test, args.force)
