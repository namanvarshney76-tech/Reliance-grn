
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
from streamlit_autorefresh import st_autorefresh

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
                    redirect_uri="https://reliancegrn.streamlit.app/"  # Update with your actual URL
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
                    
                    progress_queue.put({'type': 'info', 'text': f"Uploaded: {final_filename}"})
                    processed_count = 1
                else:
                    progress_queue.put({'type': 'info', 'text': f"File already exists, skipping: {final_filename}"})
                
            except Exception as e:
                progress_queue.put({'type': 'error', 'text': f"Failed to process attachment {filename}: {str(e)}"})
        
        return processed_count
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        import re
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _classify_extension(self, filename: str) -> str:
        """Categorize file by extension"""
        if not filename or '.' not in filename:
            return "Other"
            
        ext = filename.split(".")[-1].lower()
        
        type_map = {
            "pdf": "PDFs",
            "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }
        
        return type_map.get(ext, "Other")
    
    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        """Check if file already exists in folder"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            return len(files) > 0
        except:
            return False
    
    def process_pdf_workflow(self, config: dict, progress_queue: queue.Queue):
        """Process PDF workflow with LlamaParse"""
        try:
            if not self._check_memory(progress_queue):
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            if not LLAMA_AVAILABLE:
                progress_queue.put({'type': 'error', 'text': "LlamaParse not available. Install with: pip install llama-cloud-services"})
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': "Starting PDF processing workflow..."})
            progress_queue.put({'type': 'progress', 'value': 20})
            
            # Setup LlamaParse
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                progress_queue.put({'type': 'error', 'text': f"Could not find agent '{config['llama_agent']}'. Check LlamaParse dashboard."})
                progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'progress', 'value': 40})
            
            # List PDF files from Drive
            pdf_files = self._list_drive_files(config['drive_folder_id'], config['days_back'], progress_queue)
            
            if not pdf_files:
                progress_queue.put({'type': 'warning', 'text': "No PDF files found in the specified folder"})
                progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': 0}})
                return
            
            progress_queue.put({'type': 'status', 'text': f"Found {len(pdf_files)} PDF files. Processing..."})
            
            # Get sheet info
            sheet_name = config['sheet_range'].split('!')[0]
            
            processed_count = 0
            for i, file in enumerate(pdf_files):
                if file['id'] in self.processed_pdfs:
                    progress_queue.put({'type': 'info', 'text': f"Skipping already processed PDF: {file['name']}"})
                    continue
                
                try:
                    progress_queue.put({'type': 'status', 'text': f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}"})
                    
                    # Download PDF
                    pdf_data = self._download_from_drive(file['id'], file['name'], progress_queue)
                    if not pdf_data:
                        continue
                    
                    # Process with LlamaParse
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(pdf_data)
                        temp_path = temp_file.name
                    
                    result = agent.extract(temp_path)
                    extracted_data = result.data
                    os.unlink(temp_path)
                    
                    # Process extracted data
                    rows = self._process_extracted_data(extracted_data, file, progress_queue)
                    if rows:
                        # Save to Google Sheets
                        self._save_to_sheets(config['spreadsheet_id'], sheet_name, rows, file['id'], progress_queue, sheet_id=self._get_sheet_id(config['spreadsheet_id'], sheet_name, progress_queue))
                        processed_count += 1
                        self.processed_pdfs.add(file['id'])
                        self._save_processed_state()
                    
                    progress = 40 + (i + 1) / len(pdf_files) * 55
                    progress_queue.put({'type': 'progress', 'value': int(progress)})
                    
                except Exception as e:
                    progress_queue.put({'type': 'error', 'text': f"Failed to process PDF {file['name']}: {str(e)}"})
            
            progress_queue.put({'type': 'progress', 'value': 100})
            progress_queue.put({'type': 'status', 'text': f"PDF workflow completed! Processed {processed_count} PDFs"})
            progress_queue.put({'type': 'done', 'result': {'success': True, 'processed': processed_count}})
            
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"PDF workflow failed: {str(e)}"})
            progress_queue.put({'type': 'done', 'result': {'success': False, 'processed': 0}})
    
    def _list_drive_files(self, folder_id: str, days_back: int, progress_queue: queue.Queue) -> List[Dict]:
        """List PDF files in Drive folder"""
        try:
            start_datetime = datetime.utcnow() - timedelta(days=days_back - 1)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime >= '{start_str}'"
            
            all_files = []
            page_token = None
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
                
                files = results.get('files', [])
                all_files.extend(files)
                
                page_token = results.get('nextPageToken', None)
                if page_token is None:
                    break
            
            return all_files
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to list files: {str(e)}"})
            return []
    
    def _download_from_drive(self, file_id: str, file_name: str, progress_queue: queue.Queue) -> bytes:
        """Download file from Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            return request.execute()
        except Exception as e:
            progress_queue.put({'type': 'error', 'text': f"Failed to download {file_name}: {str(e)}"})
            return b""
    
    def _process_extracted_data(self, extracted_data: Dict, file_info: Dict, progress_queue: queue.Queue) -> List[Dict]:
        """Process extracted data from LlamaParse based on Reliance JSON structure"""
        rows = []
        items = []
        
        # Handle the provided JSON structure
        if "items" in extracted_data:
            items = extracted_data["items"]
            for item in items:
                item["po_number"] = self._get_value(extracted_data, ["po_number", "purchase_order_number", "PO No"])
                item["vendor_invoice_number"] = self._get_value(extracted_data, ["vendor_invoice_number", "invoice_number", "inv_no", "Invoice No"])
                item["supplier"] = self._get_value(extracted_data, ["Supplier Name", "supplier", "vendor"])
                item["shipping_address"] = self._get_value(extracted_data, ["delivery_address", "shipping_address", "receiver_address"])
                item["grn_date"] = self._get_value(extracted_data, ["grn_date", "delivered_on"])
                item["grn_number"] = self._get_value(extracted_data, ["grn_number"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']
        else:
            progress_queue.put({'type': 'warning', 'text': f"Skipping (no 'items' key found): {file_info['name']}"})
            return rows
        
        # Clean items and add to rows
        for item in items:
            cleaned_item = {k: v for k, v in item.items() if v not in ["", None]}
            rows.append(cleaned_item)
        
        return rows
    
    def _get_value(self, data, possible_keys, default=""):
        """Return the first found key value from dict."""
        for key in possible_keys:
            if key in data:
                return data[key]
        return default
    
    def _save_to_sheets(self, spreadsheet_id: str, sheet_name: str, rows: List[Dict], file_id: str, progress_queue: queue.Queue, sheet_id: int):
        """Save data to Google Sheets with proper header management and row replacement"""
        try:
            if not rows:
                return
            
            # Get existing headers and data
            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name, progress_queue)
            
            # Get all unique headers from new data
            new_headers = list(set().union(*(row.keys() for row in rows)))
            
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
            'days_back': 1
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
            'queue': queue.Queue()
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
        
        pdf_submit = st.form_submit_button("Update PDF Settings")
        
        if pdf_submit:
            st.session_state.pdf_config = {
                'drive_folder_id': pdf_folder,
                'llama_api_key': pdf_api_key,
                'llama_agent': pdf_agent,
                'spreadsheet_id': pdf_sheet_id,
                'sheet_range': pdf_sheet_range,
                'days_back': pdf_days
            }
            st.success("PDF settings updated!")
    
    # Add a separator
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Execute Workflows")
    st.sidebar.info("Configure settings above, then choose a workflow to run")
    
    # Main content area - workflow buttons
    st.header("Choose Workflow")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Gmail Workflow Only", use_container_width=True, 
                    disabled=st.session_state.workflow_state['running']):
            st.session_state.workflow_state['type'] = "gmail"
    
    with col2:
        if st.button("PDF Workflow Only", use_container_width=True, 
                    disabled=st.session_state.workflow_state['running']):
            st.session_state.workflow_state['type'] = "pdf"
    
    with col3:
        if st.button("Combined Workflow", use_container_width=True, 
                    disabled=st.session_state.workflow_state['running']):
            st.session_state.workflow_state['type'] = "combined"
    
    # Show current configuration preview
    if not st.session_state.workflow_state['type'] and not st.session_state.workflow_state['running']:
        st.header("Current Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Gmail Configuration")
            st.json(st.session_state.gmail_config)
        
        with col2:
            st.subheader("PDF Configuration")
            # Hide API key in display
            display_pdf_config = st.session_state.pdf_config.copy()
            display_pdf_config['llama_api_key'] = "*" * len(display_pdf_config['llama_api_key'])
            st.json(display_pdf_config)
        
        st.info("Configure your settings in the sidebar, then select a workflow above to begin automation")
        return
    
    # Run workflows using session state configurations
    if st.session_state.workflow_state['type'] and not st.session_state.workflow_state['running']:
        # Create automation instance
        automation = RelianceAutomation()
        
        # Authentication section
        st.header("Authentication")
        auth_progress = st.progress(0)
        auth_status = st.empty()
        
        if automation.authenticate_from_secrets(auth_progress, auth_status, st.session_state.workflow_state['queue']):
            st.success("Authentication successful!")
            
            # Workflow execution section
            st.header("Workflow Execution")
            
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
        # Enable auto-refresh every 1 second while running
        st_autorefresh(interval=1000, key="workflow_refresh")
        
        # Poll the queue for updates
        while not st.session_state.workflow_state['queue'].empty():
            msg = st.session_state.workflow_state['queue'].get()
            if msg['type'] == 'progress':
                st.session_state.workflow_state['progress'] = msg['value']
            elif msg['type'] == 'status':
                st.session_state.workflow_state['status'] = msg['text']
            elif msg['type'] == 'info':
                st.session_state.workflow_state['logs'].append(f"INFO: {msg['text']}")
            elif msg['type'] == 'warning':
                st.session_state.workflow_state['logs'].append(f"WARNING: {msg['text']}")
            elif msg['type'] == 'error':
                st.session_state.workflow_state['logs'].append(f"ERROR: {msg['text']}")
            elif msg['type'] == 'success':
                st.session_state.workflow_state['logs'].append(f"SUCCESS: {msg['text']}")
            elif msg['type'] == 'done':
                st.session_state.workflow_state['result'] = msg['result']
                st.session_state.workflow_state['running'] = False
        
        # Progress tracking
        main_progress = st.progress(st.session_state.workflow_state['progress'])
        main_status = st.text(st.session_state.workflow_state['status'])
        
        # Log container
        st.subheader("Real-time Logs")
        log_container = st.empty()
        log_container.text_area("Logs", "\n".join(st.session_state.workflow_state['logs'][-50:]), height=200)
        
        # Check if workflow is done
        if not st.session_state.workflow_state['running']:
            # Clean up thread
            thread = st.session_state.workflow_state['thread']
            if thread and thread.is_alive():
                thread.join()
            
            # Show result
            result = st.session_state.workflow_state['result']
            if result and result['success']:
                st.success(f"{st.session_state.workflow_state['type'].capitalize()} workflow completed! Processed {result['processed']} items")
                if st.session_state.workflow_state['type'] == "combined":
                    st.balloons()
            elif result:
                st.error(f"{st.session_state.workflow_state['type'].capitalize()} workflow failed")
            
            # Reset button
            if st.button("Reset Workflow"):
                st.session_state.workflow_state['type'] = None
                st.session_state.workflow_state['result'] = None
                st.rerun()

    # Reset all settings
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Reset Workflow", use_container_width=True):
            st.session_state.workflow_state['type'] = None
            st.session_state.workflow_state['result'] = None
            st.rerun()
    with col2:
        if st.button("Reset All Settings", use_container_width=True, type="secondary"):
            for key in ['gmail_config', 'pdf_config', 'workflow_state']:
                if key in st.session_state:
                    del st.session_state[key]
            if os.path.exists("processed_state.json"):
                os.remove("processed_state.json")
            st.rerun()

if __name__ == "__main__":
    main()

