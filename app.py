#!/usr/bin/env python3
"""
Streamlit App for Reliance Automation Workflows
Combines Gmail attachment downloader and PDF processor with real-time tracking
"""
import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from io import StringIO
import threading
import queue
import psutil
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import io

# Try to import LlamaParse
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

# Configure Streamlit page
st.set_page_config(
    page_title="Reliance Automation",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

class RelianceAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        self.processed_state_file = "processed_state.json"
        self.processed_emails = set()
        self.processed_pdfs = set()
        
        # Load processed state
        self._load_processed_state()
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive.file']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def _load_processed_state(self):
        """Load previously processed email and PDF IDs from file"""
        try:
            if os.path.exists(self.processed_state_file):
                with open(self.processed_state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_emails = set(state.get('emails', []))
                    self.processed_pdfs = set(state.get('pdfs', []))
        except Exception as e:
            pass
    
    def _save_processed_state(self):
        """Save processed email and PDF IDs to file"""
        try:
            state = {
                'emails': list(self.processed_emails),
                'pdfs': list(self.processed_pdfs)
            }
            with open(self.processed_state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            pass
    
    def _check_memory(self, progress_queue: queue.Queue):
        """Check memory usage to prevent crashes"""
        process = psutil.Process()
        mem_info = process.memory_info()
        if mem_info.rss > 0.8 * psutil.virtual_memory().total:  # 80% of total memory
            progress_queue.put({'type': 'error', 'text': "Memory usage too high, stopping to prevent crash"})
            return False
        return True
    
    def authenticate_from_secrets(self, progress_bar, status_text, progress_queue: queue.Queue):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    progress_queue.put({'type': 'info', 'text': f"Cached token invalid, requesting new authentication: {str(e)}"})
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri="https://reliancegrn.streamlit.app/"
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        progress_queue.put({'type': 'error', 'text': f"Authentication failed: {str(e)}"})
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Authorize with Google]({auth_url})")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                progress_queue.put({'type': 'error', 'text': "Google credentials missing in Streamlit secrets"})
                return False
                
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Authentication failed: {str(e)}"})
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "",
                     days_back: int = 7, max_results: int = 50, progress_queue: queue.Queue = None) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            # Build search query
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            # Add date filter
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            progress_queue.put({'type': 'info', 'text': f"Searching Gmail with query: {query}"})
            
            # Execute search
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            progress_queue.put({'type': 'info', 'text': f"Gmail search returned {len(messages)} messages"})
            
            # Debug: Show some email details
            if messages:
                progress_queue.put({'type': 'info', 'text': "Sample emails found:"})
                for i, msg in enumerate(messages[:3]):  # Show first 3 emails
                    try:
                        email_details = self._get_email_details(msg['id'], progress_queue)
                        progress_queue.put({'type': 'info', 'text': f" {i+1}. {email_details['subject']} from {email_details['sender']}"})
                    except:
                        progress_queue.put({'type': 'info', 'text': f" {i+1}. Email ID: {msg['id']}"})
            
            return messages
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Email search failed: {str(e)}"})
            return []
    
    def process_gmail_workflow(self, config: dict, progress_queue: queue.Queue):
        """Process Gmail attachment download workflow"""
        try:
            if not self._check_memory(progress_queue):
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': "Starting Gmail workflow..."})
            progress_queue.put({'type': 'progress', 'value': 10})
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results'],
                progress_queue=progress_queue
            )
            
            progress_queue.put({'type': 'progress', 'value': 25})
            
            if not emails:
                progress_queue.put({'type': 'warning', 'text': "No emails found matching criteria"})
                progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': f"Found {len(emails)} emails. Processing attachments..."})
            progress_queue.put({'type': 'info', 'text': f"Found {len(emails)} emails matching criteria"})
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'), progress_queue)
            
            if not base_folder_id:
                progress_queue.put({'type': 'error', 'text': "Failed to create base folder in Google Drive"})
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'progress', 'value': 50})
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                if email['id'] in self.processed_emails:
                    progress_queue.put({'type': 'info', 'text': f"Skipping already processed email ID: {email['id']}"})
                    continue
                
                try:
                    progress_queue.put({'type': 'status', 'text': f"Processing email {i+1}/{len(emails)}"})
                    
                    # Get email details
                    email_details = self._get_email_details(email['id'], progress_queue)
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    progress_queue.put({'type': 'info', 'text': f"Processing email: {subject} from {sender}"})
                    
                    # Get full message with payload
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        progress_queue.put({'type': 'warning', 'text': f"No payload found for email: {subject}"})
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], config, base_folder_id, progress_queue
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self.processed_emails.add(email['id'])
                        self._save_processed_state()
                        progress_queue.put({'type': 'success', 'text': f"Found {attachment_count} attachments in: {subject}"})
                    else:
                        progress_queue.put({'type': 'info', 'text': f"No matching attachments in: {subject}"})
                    
                    progress = 50 + (i + 1) / len(emails) * 45
                    progress_queue.put({'type': 'progress', 'value': int(progress)})
                    
                except Exception as e:
                    progress_queue.put({'type': 'error', 'text': f"Failed to process email {email.get('id', 'unknown')}: {str(e)}"})
            
            progress_queue.put({'type': 'progress', 'value': 100})
            progress_queue.put({'type': 'status', 'text': f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails"})
            progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': total_attachments}})
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Gmail workflow failed: {str(e)}"})
            progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
    
    def _get_email_details(self, message_id: str, progress_queue: queue.Queue) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to get email details for {message_id}: {str(e)}"})
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None, progress_queue: queue.Queue = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to create folder {folder_name}: {str(e)}"})
            return ""
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, config: dict, base_folder_id: str, progress_queue: queue.Queue) -> int:
        """Extract attachments from email with proper folder structure"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, config, base_folder_id, progress_queue
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create nested folder structure: Gmail_Attachments -> search_term -> file_type
                search_term = config.get('search_term', 'all-attachments')
                search_folder_name = search_term if search_term else "all-attachments"
                file_type_folder = self._classify_extension(filename)
                
                # Create search term folder
                search_folder_id = self._create_drive_folder(search_folder_name, base_folder_id, progress_queue)
                
                # Create file type folder within search folder
                type_folder_id = self._create_drive_folder(file_type_folder, search_folder_id, progress_queue)
                
                # Clean filename but do not add prefix
                clean_filename = self._sanitize_filename(filename)
                final_filename = clean_filename
                
                # Check if file already exists
                if not self._file_exists_in_folder(final_filename, type_folder_id):
                    # Upload to Drive
                    file_metadata = {
                        'name': final_filename,
                        'parents': [type_folder_id]
                    }
                    
                    media = MediaIoBaseUpload(
                        io.BytesIO(file_data),
                        mimetype='application/octet-stream',
                        resumable=True
                    )
                    
                    self.drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()
                    
                    progress_queue.put({'type': 'success', 'text': f"Uploaded attachment: {final_filename}"})
                    processed_count = 1
                
            except Exception as e:
                progress_queue.put({'type': 'error', 'text': f"Failed to process attachment {filename}: {str(e)}"})
            
        return processed_count

    def _classify_extension(self, filename: str) -> str:
        ext = filename.lower().split('.')[-1] if '.' in filename else 'unknown'
        if ext in ['pdf']:
            return 'pdf'
        elif ext in ['jpg', 'jpeg', 'png', 'gif']:
            return 'images'
        elif ext in ['doc', 'docx', 'txt']:
            return 'documents'
        else:
            return 'others'
    
    def _sanitize_filename(self, filename: str) -> str:
        return "".join([c for c in filename if c.isalpha() or c.isdigit() or c in ' ._-']).strip()
    
    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query).execute()
        return len(results.get('files', [])) > 0
    
    def _get_existing_pdf_ids(self, spreadsheet_id: str, sheet_name: str, progress_queue: queue.Queue) -> set:
        """Get set of existing drive_file_id from Google Sheet"""
        try:
            values = self._get_sheet_data(spreadsheet_id, sheet_name, progress_queue)
            if not values:
                return set()
            
            headers = values[0]
            if 'drive_file_id' not in headers:
                progress_queue.put({'type': 'warning', 'text': "No 'drive_file_id' column found in sheet"})
                return set()
            
            id_col = headers.index('drive_file_id')
            existing_ids = {row[id_col] for row in values[1:] if len(row) > id_col and row[id_col]}
            
            progress_queue.put({'type': 'info', 'text': f"Found {len(existing_ids)} existing file IDs in sheet"})
            return existing_ids
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to get existing file IDs: {str(e)}"})
            return set()
    
    def process_pdf_workflow(self, config: dict, progress_queue: queue.Queue):
        """Process PDF extraction workflow"""
        try:
            if not LLAMA_AVAILABLE:
                progress_queue.put({'type': 'error', 'text': "LlamaParse not available"})
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            if not self._check_memory(progress_queue):
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': "Starting PDF workflow..."})
            progress_queue.put({'type': 'progress', 'value': 10})
            
            # Get existing IDs from sheet
            sheet_name = config['sheet_range']
            existing_ids = self._get_existing_pdf_ids(config['spreadsheet_id'], sheet_name, progress_queue)
            
            # List PDF files from Drive
            pdf_files = self._list_drive_pdfs(config['drive_folder_id'], config['days_back'], progress_queue)
            
            progress_queue.put({'type': 'progress', 'value': 25})
            
            if not pdf_files:
                progress_queue.put({'type': 'warning', 'text': "No PDFs found in Drive folder"})
                progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': 0}})
                return
            
            # Filter to new files only
            new_pdfs = [f for f in pdf_files if f['id'] not in existing_ids and f['id'] not in self.processed_pdfs]
            
            # Apply max_files limit
            max_files = config.get('max_files', len(new_pdfs))
            new_pdfs = new_pdfs[:max_files]
            
            progress_queue.put({'type': 'info', 'text': f"Found {len(pdf_files)} PDFs, {len(new_pdfs)} new to process (limited to {max_files})"})
            
            if not new_pdfs:
                progress_queue.put({'type': 'info', 'text': "All PDFs already processed"})
                progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': f"Processing {len(new_pdfs)} new PDFs..."})
            
            # Set up LlamaParse
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if not agent:
                progress_queue.put({'type': 'error', 'text': f"Failed to get Llama agent '{config['llama_agent']}'"})
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            processed_count = 0
            total_rows = 0
            
            for i, file in enumerate(new_pdfs):
                try:
                    progress_queue.put({'type': 'status', 'text': f"Processing PDF {i+1}/{len(new_pdfs)}: {file['name']}"})
                    
                    # Download PDF
                    pdf_data = self._download_drive_file(file['id'], progress_queue)
                    if not pdf_data:
                        continue
                    
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                        tmp.write(pdf_data)
                        tmp_path = tmp.name
                    
                    # Extract with Llama
                    result = agent.extract(tmp_path)
                    os.unlink(tmp_path)
                    
                    if not result or not result.data:
                        progress_queue.put({'type': 'warning', 'text': f"No data extracted from {file['name']}"})
                        continue
                    
                    # Process extracted data
                    rows = self._process_extracted_data(result.data, file)
                    if rows:
                        self._save_to_sheets(config['spreadsheet_id'], sheet_name, rows, file['id'], progress_queue)
                        total_rows += len(rows)
                        processed_count += 1
                        self.processed_pdfs.add(file['id'])
                        self._save_processed_state()
                        progress_queue.put({'type': 'success', 'text': f"Processed {file['name']}, added {len(rows)} rows"})
                    
                    progress = 25 + (i + 1) / len(new_pdfs) * 70
                    progress_queue.put({'type': 'progress', 'value': int(progress)})
                    
                except Exception as e:
                    progress_queue.put({'type': 'error', 'text': f"Failed to process PDF {file.get('name', 'unknown')}: {str(e)}"})
            
            progress_queue.put({'type': 'progress', 'value': 100})
            progress_queue.put({'type': 'status', 'text': f"PDF workflow completed! Processed {processed_count} PDFs, added {total_rows} rows"})
            progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': processed_count, 'rows_added': total_rows}})
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"PDF workflow failed: {str(e)}"})
            progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
    
    def _list_drive_pdfs(self, folder_id: str, days_back: int, progress_queue: queue.Queue) -> List[Dict]:
        """List PDF files in Drive folder created in last N days"""
        try:
            start_time = (datetime.now() - timedelta(days=days_back)).isoformat() + 'Z'
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime > '{start_time}'"
            results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            progress_queue.put({'type': 'info', 'text': f"Found {len(files)} PDFs in Drive"})
            return files
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to list Drive PDFs: {str(e)}"})
            return []
    
    def _download_drive_file(self, file_id: str, progress_queue: queue.Queue) -> Optional[bytes]:
        """Download file from Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return fh.getvalue()
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to download file {file_id}: {str(e)}"})
            return None
    
    def _process_extracted_data(self, data: Any, file: Dict) -> List[Dict]:
        """Process Llama extracted data into rows"""
        rows = []
        for item in data:
            row = item.copy()
            row['drive_file_id'] = file['id']
            row['file_name'] = file['name']
            row['processed_at'] = datetime.now().isoformat()
            rows.append(row)
        return rows
    
    def _save_to_sheets(self, spreadsheet_id: str, sheet_name: str, rows: List[Dict], file_id: str, progress_queue: queue.Queue):
        """Save extracted rows to Google Sheets, handling dynamic headers"""
        try:
            sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name, progress_queue)
            if not sheet_id:
                return
            
            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name, progress_queue)
            
            new_headers = list(set([k for row in rows for k in row.keys()]))
            new_headers.sort()  # Consistent order
            
            # Combine headers (existing + new unique ones)
            if existing_headers:
                all_headers = existing_headers.copy()
                for header in new_headers:
                    if header not in all_headers:
                        all_headers.append(header)
                
                # Update headers if new ones were added
                if len(all_headers) > len(existing_headers):
                    self._update_headers(spreadsheet_id, sheet_name, all_headers, progress_queue)
            else:
                # No existing headers, create them
                all_headers = new_headers
                self._update_headers(spreadsheet_id, sheet_name, all_headers, progress_queue)
            
            # Prepare values
            values = [[row.get(h, "") for h in all_headers] for row in rows]
            
            # Replace rows for this specific file
            self._replace_rows_for_file(spreadsheet_id, sheet_name, file_id, all_headers, values, sheet_id, progress_queue)
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to save to sheets: {str(e)}"})
    
    def _get_sheet_headers(self, spreadsheet_id: str, sheet_name: str, progress_queue: queue.Queue) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            progress_queue.put({'type': 'info', 'text': f"No existing headers found: {str(e)}"})
            return []
    
    def _update_headers(self, spreadsheet_id: str, sheet_name: str, headers: List[str], progress_queue: queue.Queue) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + len(headers))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            progress_queue.put({'type': 'info', 'text': f"Updated headers with {len(headers)} columns"})
            return True
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to update headers: {str(e)}"})
            return False
    
    def _get_sheet_id(self, spreadsheet_id: str, sheet_name: str, progress_queue: queue.Queue) -> int:
        """Get the numeric sheet ID for the given sheet name"""
        try:
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for sheet in metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            progress_queue.put({'type': 'warning', 'text': f"Sheet '{sheet_name}' not found"})
            return 0
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to get sheet metadata: {str(e)}"})
            return 0
    
    def _get_sheet_data(self, spreadsheet_id: str, sheet_name: str, progress_queue: queue.Queue) -> List[List[str]]:
        """Get all data from the sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                majorDimension="ROWS"
            ).execute()
            return result.get('values', [])
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to get sheet data: {str(e)}"})
            return []
    
    def _replace_rows_for_file(self, spreadsheet_id: str, sheet_name: str, file_id: str,
                             headers: List[str], new_rows: List[List[Any]], sheet_id: int, progress_queue: queue.Queue) -> bool:
        """Delete existing rows for the file if any, and append new rows"""
        try:
            values = self._get_sheet_data(spreadsheet_id, sheet_name, progress_queue)
            if not values:
                # No existing data, just append
                return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows, progress_queue)
            
            current_headers = values[0]
            data_rows = values[1:]
            
            # Find file_id column
            try:
                file_id_col = current_headers.index('drive_file_id')
            except ValueError:
                progress_queue.put({'type': 'info', 'text': "No 'drive_file_id' column found, appending new rows"})
                return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows, progress_queue)
            
            # Find rows to delete (matching file_id)
            rows_to_delete = []
            for idx, row in enumerate(data_rows, 2):  # Start from row 2 (after header)
                if len(row) > file_id_col and row[file_id_col] == file_id:
                    rows_to_delete.append(idx)
            
            # Delete existing rows for this file
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)  # Delete from bottom to top
                requests = []
                for row_idx in rows_to_delete:
                    requests.append({
                        'deleteDimension': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,  # 0-indexed
                                'endIndex': row_idx
                            }
                        }
                    })
                
                if requests:
                    body = {'requests': requests}
                    self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=body
                    ).execute()
                    progress_queue.put({'type': 'info', 'text': f"Deleted {len(rows_to_delete)} existing rows for file {file_id}"})
            
            # Append new rows
            return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows, progress_queue)
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to replace rows: {str(e)}"})
            return False
    
    def _append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]], progress_queue: queue.Queue) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                progress_queue.put({'type': 'info', 'text': f"Appended {updated_cells} cells to Google Sheet"})
                return True
            except Exception as e:
                if attempt < max_retries:
                    progress_queue.put({'type': 'warning', 'text': f"Failed to append to Google Sheet (attempt {attempt}/{max_retries}): {str(e)}"})
                    time.sleep(wait_time)
                else:
                    progress_queue.put({'type': 'error', 'text': f"Failed to append to Google Sheet after {max_retries} attempts: {str(e)}"})
                    return False
        return False

def run_workflow_in_background(automation, workflow_type, gmail_config, pdf_config, progress_queue):
    """Run the selected workflow in background, sending updates to queue"""
    try:
        if workflow_type == "gmail":
            automation.process_gmail_workflow(gmail_config, progress_queue)
        elif workflow_type == "pdf":
            automation.process_pdf_workflow(pdf_config, progress_queue)
        elif workflow_type == "combined":
            progress_queue.put({'type': 'info', 'text': "Running combined workflow..."})
            progress_queue.put({'type': 'status', 'text': "Step 1: Gmail Attachment Download"})
            automation.process_gmail_workflow(gmail_config, progress_queue)
            time.sleep(2)  # Small delay between steps
            progress_queue.put({'type': 'status', 'text': "Step 2: PDF Processing"})
            automation.process_pdf_workflow(pdf_config, progress_queue)
            progress_queue.put({'type': 'success', 'text': "Combined workflow completed successfully!"})
    except Exception as e:
        progress_queue.put({'type': 'error', 'text': f"Workflow execution failed: {str(e)}"})
        progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})

def main():
    st.title("⚡ Reliance Automation Dashboard")
    st.markdown("Automate Gmail attachment downloads and PDF processing workflows")
    
    # Initialize session state for configuration
    if 'gmail_config' not in st.session_state:
        st.session_state.gmail_config = {
            'sender': "DONOTREPLY@ril.com",
            'search_term': "grn",
            'days_back': 7,
            'max_results': 1000,
            'gdrive_folder_id': "1YH8bT01X0C03SbgFF8qWO49Tv85Xd5UU"
        }
    
    if 'pdf_config' not in st.session_state:
        st.session_state.pdf_config = {
            'drive_folder_id': "1CKPlXQcQsvGDWmpINVj8lpKI7G9VG1Yv",
            'llama_api_key': "llx-rK35vMeW6MVmM9nVpbfdMfiZZzRoBmrLsC3EfiCm1qamfQ5p",
            'llama_agent': "Reliance Agent",
            'spreadsheet_id': "1zlJaRur0K50ZLFQhxxmvfFVA3l4Whpe9XWgi1E-HFhg",
            'sheet_range': "reliancegrn",
            'days_back': 1,
            'max_files': 50
        }
    
    # Initialize workflow state
    if 'workflow_state' not in st.session_state:
        st.session_state.workflow_state = {
            'running': False,
            'type': None,
            'progress': 0,
            'status': '',
            'logs': [],
            'result': None,
            'thread': None,
            'queue': queue.Queue(),
            'authenticated': False
        }
    
    # Configuration section in sidebar
    st.sidebar.header("Configuration")
    
    with st.sidebar.form("gmail_config_form"):
        st.subheader("Gmail Settings")
        gmail_sender = st.text_input("Sender Email", value=st.session_state.gmail_config['sender'])
        gmail_search = st.text_input("Search Term", value=st.session_state.gmail_config['search_term'])
        gmail_days = st.number_input("Days Back", value=st.session_state.gmail_config['days_back'], min_value=1)
        gmail_max = st.number_input("Max Results", value=st.session_state.gmail_config['max_results'], min_value=1)
        gmail_folder = st.text_input("Google Drive Folder ID", value=st.session_state.gmail_config['gdrive_folder_id'])
        
        gmail_submit = st.form_submit_button("Update Gmail Settings")
        
        if gmail_submit:
            st.session_state.gmail_config = {
                'sender': gmail_sender,
                'search_term': gmail_search,
                'days_back': gmail_days,
                'max_results': gmail_max,
                'gdrive_folder_id': gmail_folder
            }
            st.success("Gmail settings updated!")
    
    with st.sidebar.form("pdf_config_form"):
        st.subheader("PDF Processing Settings")
        pdf_folder = st.text_input("PDF Drive Folder ID", value=st.session_state.pdf_config['drive_folder_id'])
        pdf_api_key = st.text_input("LlamaParse API Key", value=st.session_state.pdf_config['llama_api_key'], type="password")
        pdf_agent = st.text_input("LlamaParse Agent", value=st.session_state.pdf_config['llama_agent'])
        pdf_sheet_id = st.text_input("Spreadsheet ID", value=st.session_state.pdf_config['spreadsheet_id'])
        pdf_sheet_range = st.text_input("Sheet Range", value=st.session_state.pdf_config['sheet_range'])
        pdf_days = st.number_input("PDF Days Back", value=st.session_state.pdf_config['days_back'], min_value=1)
        pdf_max_files = st.number_input("Max Files to Process", value=st.session_state.pdf_config['max_files'], min_value=1)
        
        pdf_submit = st.form_submit_button("Update PDF Settings")
        
        if pdf_submit:
            st.session_state.pdf_config = {
                'drive_folder_id': pdf_folder,
                'llama_api_key': pdf_api_key,
                'llama_agent': pdf_agent,
                'spreadsheet_id': pdf_sheet_id,
                'sheet_range': pdf_sheet_range,
                'days_back': pdf_days,
                'max_files': pdf_max_files
            }
            st.success("PDF settings updated!")
    
    # Add a separator
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Execute Workflows")
    st.sidebar.info("Configure settings above, then choose a workflow tab to run")
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📧 Gmail Workflow", "📄 PDF Workflow", "🔗 Combined Workflow", "📋 Logs"])
    
    with tab1:
        st.header("📧 Gmail Workflow")
        st.markdown("Download attachments from Gmail to Drive")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Current Gmail Configuration")
            st.json(st.session_state.gmail_config)
        
        if st.session_state.workflow_state['running']:
            st.warning("Workflow is running...")
        elif st.session_state.workflow_state['result']:
            result = st.session_state.workflow_state['result']
            if result['success']:
                st.success(f"Gmail workflow completed! Processed {result['processed']} attachments")
            else:
                st.error("Gmail workflow failed. Check logs for details.")
        else:
            if st.button("Start Gmail Workflow", key="start_gmail", type="primary"):
                st.session_state.workflow_state['type'] = "gmail"
                st.session_state.workflow_state['result'] = None
                st.rerun()
    
    with tab2:
        st.header("📄 PDF Workflow")
        st.markdown("Process PDFs from Drive to Sheets")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Current PDF Configuration")
            display_pdf_config = st.session_state.pdf_config.copy()
            display_pdf_config['llama_api_key'] = "*" * len(display_pdf_config['llama_api_key'])
            st.json(display_pdf_config)
        
        if st.session_state.workflow_state['running']:
            st.warning("Workflow is running...")
        elif st.session_state.workflow_state['result']:
            result = st.session_state.workflow_state['result']
            if result['success']:
                st.success(f"PDF workflow completed! Processed {result['processed']} PDFs, added {result.get('rows_added', 0)} rows")
            else:
                st.error("PDF workflow failed. Check logs for details.")
        else:
            if st.button("Start PDF Workflow", key="start_pdf", type="primary"):
                st.session_state.workflow_state['type'] = "pdf"
                st.session_state.workflow_state['result'] = None
                st.rerun()
    
    with tab3:
        st.header("🔗 Combined Workflow")
        st.markdown("Run Gmail then PDF workflow")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Current Configurations")
            st.json(st.session_state.gmail_config)
            display_pdf_config = st.session_state.pdf_config.copy()
            display_pdf_config['llama_api_key'] = "*" * len(display_pdf_config['llama_api_key'])
            st.json(display_pdf_config)
        
        if st.session_state.workflow_state['running']:
            st.warning("Workflow is running...")
        elif st.session_state.workflow_state['result']:
            result = st.session_state.workflow_state['result']
            if result['success']:
                st.success(f"Combined workflow completed! Processed {result['processed']} items")
                st.balloons()
            else:
                st.error("Combined workflow failed. Check logs for details.")
        else:
            if st.button("Start Combined Workflow", key="start_combined", type="primary"):
                st.session_state.workflow_state['type'] = "combined"
                st.session_state.workflow_state['result'] = None
                st.rerun()
    
    with tab4:
        st.header("📋 Logs")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh Logs"):
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Logs"):
                st.session_state.workflow_state['logs'] = []
                st.success("Logs cleared!")
                st.rerun()
        with col3:
            if st.checkbox("Auto-refresh (5s)"):
                time.sleep(5)
                st.rerun()
        
        logs = st.session_state.workflow_state['logs']
        if logs:
            st.subheader(f"Recent Activity ({len(logs)} entries)")
            for log_entry in reversed(logs[-50:]):
                timestamp = log_entry['timestamp']
                level = log_entry['level']
                message = log_entry['message']
                if level == "ERROR":
                    st.error(f"🔴 **{timestamp}** - {message}")
                elif level == "WARNING":
                    st.warning(f"🟡 **{timestamp}** - {message}")
                elif level == "SUCCESS":
                    st.success(f"🟢 **{timestamp}** - {message}")
                else:
                    st.info(f"ℹ️ **{timestamp}** - {message}")
        else:
            st.info("No logs available. Start a workflow to see activity logs.")
    
    # Handle workflow execution
    if st.session_state.workflow_state['type'] and not st.session_state.workflow_state['running']:
        # Authentication
        if not st.session_state.workflow_state['authenticated']:
            st.header("Authentication")
            auth_progress = st.progress(0)
            auth_status = st.empty()
            
            # Create automation instance
            if 'automation' not in st.session_state:
                st.session_state.automation = RelianceAutomation()
            
            automation = st.session_state.automation
            
            if automation.authenticate_from_secrets(auth_progress, auth_status, st.session_state.workflow_state['queue']):
                st.success("Authentication successful!")
                st.session_state.workflow_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Authentication failed")
                st.session_state.workflow_state['type'] = None
                st.session_state.workflow_state['result'] = None
                st.rerun()
            return
        
        # Start workflow
        automation = st.session_state.automation
        
        # Workflow execution section
        st.header("Workflow Execution")
        main_progress = st.progress(0)
        main_status = st.text("Initializing...")
        log_container = st.text_area("Real-time Logs", height=200)
        
        # Start the background thread
        thread = threading.Thread(
            target=run_workflow_in_background,
            args=(automation, st.session_state.workflow_state['type'], 
                  st.session_state.gmail_config, st.session_state.pdf_config, 
                  st.session_state.workflow_state['queue'])
        )
        thread.start()
        
        # Update workflow state
        st.session_state.workflow_state['running'] = True
        st.session_state.workflow_state['thread'] = thread
        st.session_state.workflow_state['logs'] = []
        st.session_state.workflow_state['progress'] = 0
        st.session_state.workflow_state['status'] = "Initializing..."
    
    # Handle running workflows
    if st.session_state.workflow_state['running']:
        # Poll the queue for updates
        while not st.session_state.workflow_state['queue'].empty():
            msg = st.session_state.workflow_state['queue'].get()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if msg['type'] == 'progress':
                st.session_state.workflow_state['progress'] = msg['value']
            elif msg['type'] == 'status':
                st.session_state.workflow_state['status'] = msg['text']
            elif msg['type'] in ['info', 'warning', 'error', 'success']:
                st.session_state.workflow_state['logs'].append({
                    'timestamp': timestamp,
                    'level': msg['type'].upper(),
                    'message': msg['text']
                })
            elif msg['type'] == 'done':
                st.session_state.workflow_state['result'] = msg['result']
                st.session_state.workflow_state['running'] = False
        
        # Update UI
        main_progress = st.progress(st.session_state.workflow_state['progress'])
        main_status = st.text(st.session_state.workflow_state['status'])
        log_container = st.empty()
        log_container.text_area("Logs", "\n".join([f"{log['timestamp']} {log['level']}: {log['message']}" for log in st.session_state.workflow_state['logs'][-50:]]), height=200)
        
        # Check if workflow is done
        if not st.session_state.workflow_state['running']:
            # Clean up thread
            thread = st.session_state.workflow_state['thread']
            if thread and thread.is_alive():
                thread.join()
            
            # Reset workflow state
            st.session_state.workflow_state['type'] = None
            st.session_state.workflow_state['authenticated'] = False
            if 'automation' in st.session_state:
                del st.session_state.automation
            
            st.rerun()

    # Reset all settings
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Reset Workflow", use_container_width=True):
            st.session_state.workflow_state['type'] = None
            st.session_state.workflow_state['result'] = None
            st.session_state.workflow_state['authenticated'] = False
            if 'automation' in st.session_state:
                del st.session_state.automation
            st.rerun()
    with col2:
        if st.button("Reset All Settings", use_container_width=True, type="secondary"):
            for key in ['gmail_config', 'pdf_config', 'workflow_state', 'automation']:
                if key in st.session_state:
                    del st.session_state[key]
            if os.path.exists("processed_state.json"):
                os.remove("processed_state.json")
            st.rerun()

if __name__ == "__main__":
    main()
