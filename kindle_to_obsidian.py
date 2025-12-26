#!/usr/bin/env python3
"""
Kindle Scribe to Obsidian Sync
Monitors Gmail for Kindle exports, extracts highlights/notes, creates individual Obsidian notes
"""

import os
import base64
import pickle
import re
import requests
from pathlib import Path
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import unquote

import PyPDF2
import pytesseract
from pdf2image import convert_from_bytes
from PIL import Image
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Configuration
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
OBSIDIAN_VAULT_PATH = "C:/Users/Lenovo/Documents/Cat's Mind Garden"
OUTPUT_FOLDER = "3 - Nonfiction"
PROCESSED_EMAILS_FILE = "processed_kindle_emails.txt"

class LinkExtractor(HTMLParser):
    """Extract download links from HTML email content"""
    def __init__(self):
        super().__init__()
        self.links = []
    
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href' and 'kindle-content-requests' in value:
                    clean_url = value.replace('=3D', '=').replace('=\n', '')
                    self.links.append(clean_url)

class KindleToObsidian:
    def __init__(self, vault_path):
        self.vault_path = Path(vault_path)
        self.output_path = self.vault_path / OUTPUT_FOLDER
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.processed_file = self.vault_path / PROCESSED_EMAILS_FILE
        self.gmail_service = None
        
    def authenticate_gmail(self):
        """Authenticate with Gmail API"""
        creds = None
        token_file = 'token.pickle'
        
        if os.path.exists(token_file):
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        self.gmail_service = build('gmail', 'v1', credentials=creds)
        print("✓ Authenticated with Gmail")
    
    def get_processed_emails(self):
        """Load list of already processed email IDs"""
        if self.processed_file.exists():
            return set(self.processed_file.read_text().strip().split('\n'))
        return set()
    
    def mark_email_processed(self, email_id):
        """Mark an email as processed"""
        with open(self.processed_file, 'a') as f:
            f.write(f"{email_id}\n")
    
    def search_kindle_emails(self):
        """Search for unprocessed Kindle export emails"""
        processed = self.get_processed_emails()
        query = 'from:do-not-reply@amazon.com (subject:notebook OR subject:kindle OR "sent a file")'
        
        results = self.gmail_service.users().messages().list(
            userId='me', q=query, maxResults=50
        ).execute()
        
        messages = results.get('messages', [])
        unprocessed = [msg for msg in messages if msg['id'] not in processed]
        
        print(f"Found {len(unprocessed)} new Kindle emails to process")
        return unprocessed
    
    def extract_download_links(self, html_content):
        """Extract Kindle download links from HTML email"""
        parser = LinkExtractor()
        parser.feed(html_content)
        
        clean_links = []
        for link in parser.links:
            match = re.search(r'U=([^&]+)', link)
            if match:
                s3_url = unquote(match.group(1))
                clean_links.append(s3_url)
            else:
                clean_links.append(link)
        
        return clean_links
    
    def download_from_link(self, url):
        """Download content from Amazon S3 link"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"    ✗ Error downloading from link: {e}")
            return None
    
    def get_email_content(self, message_id):
        """Get email content including body and attachments"""
        message = self.gmail_service.users().messages().get(
            userId='me', id=message_id, format='full'
        ).execute()
        
        headers = message['payload']['headers']
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'Unknown')
        
        html_content = ""
        if 'parts' in message['payload']:
            for part in message['payload']['parts']:
                if part['mimeType'] == 'text/html' and 'data' in part['body']:
                    html_content = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                    break
        elif 'body' in message['payload'] and 'data' in message['payload']['body']:
            html_content = base64.urlsafe_b64decode(message['payload']['body']['data']).decode('utf-8')
        
        parts = message['payload'].get('parts', [])
        for part in parts:
            if part.get('filename', '').endswith('.pdf'):
                attachment_id = part['body']['attachmentId']
                attachment = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                pdf_data = base64.urlsafe_b64decode(attachment['data'])
                return {'type': 'pdf', 'data': pdf_data, 'filename': part['filename'], 'subject': subject}
        
        links = self.extract_download_links(html_content)
        if links:
            return {'type': 'link', 'links': links, 'subject': subject}
        
        return {'type': 'none', 'subject': subject}
    
    def extract_text_from_pdf(self, pdf_data):
        """Extract text from PDF bytes using both text extraction and OCR"""
        from io import BytesIO
        
        text = ""
        
        try:
            pdf_file = BytesIO(pdf_data)
            reader = PyPDF2.PdfReader(pdf_file)
            
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text and page_text.strip():
                    text += page_text + "\n"
            
            print(f"    - Extracted {len(text)} chars via text extraction")
        except Exception as e:
            print(f"    - Text extraction failed: {e}")
        
        if len(text.strip()) < 100:
            print(f"    - Low text content, attempting OCR...")
            try:
                images = convert_from_bytes(pdf_data)
                ocr_text = ""
                
                for i, image in enumerate(images):
                    page_text = pytesseract.image_to_string(image)
                    if page_text.strip():
                        ocr_text += page_text + "\n"
                    print(f"      Page {i+1}: {len(page_text)} chars")
                
                if len(ocr_text) > len(text):
                    text = ocr_text
                    print(f"    - OCR extracted {len(text)} chars total")
            except Exception as e:
                print(f"    - OCR failed: {e}")
        
        return text
    
    def parse_highlights_and_notes(self, text, source_title):
        """Parse text into individual highlights/notes"""
        notes = []
        
        text = re.sub(r'Page \d+\s*\n', '', text)
        chunks = re.split(r'\n{2,}', text)
        
        for i, chunk in enumerate(chunks):
            chunk = chunk.strip()
            if len(chunk) > 10:
                title = None
                content = chunk
                
                title_match = re.match(r'^Title:\s*(.+?)(?:\n|$)', chunk, re.IGNORECASE)
                if title_match:
                    title = title_match.group(1).strip()
                    content = re.sub(r'^Title:\s*.+?(?:\n|$)', '', chunk, flags=re.IGNORECASE).strip()
                
                if not title:
                    first_line = content.split('\n')[0]
                    title = first_line[:50] if len(first_line) > 50 else first_line
                
                lines = content.split('\n')
                cleaned_lines = []
                
                for j, line in enumerate(lines):
                    line = line.strip()
                    if not line:
                        cleaned_lines.append('')
                        continue
                    
                    ends_with_punct = bool(re.search(r'[.!?:;,]$', line))
                    is_last = j == len(lines) - 1
                    
                    if not ends_with_punct and not is_last:
                        cleaned_lines.append(line + ' ')
                    else:
                        cleaned_lines.append(line)
                
                content = ''.join(cleaned_lines)
                content = re.sub(r' {2,}', ' ', content)
                content = re.sub(r'\n{3,}', '\n\n', content)
                content = content.strip()
                
                notes.append({
                    'title': title,
                    'content': content,
                    'source': source_title,
                    'index': i + 1
                })
        
        return notes
    
    def create_obsidian_note(self, note_data):
        """Create individual Obsidian markdown note"""
        title = note_data['title']
        content = note_data['content']
        
        title_clean = re.sub(r'[^\w\s-]', '', title)
        title_clean = re.sub(r'\s+', ' ', title_clean).strip()
        
        if not title_clean:
            title_clean = f"Note {note_data['index']}"
        
        filename = f"{title_clean}.md"
        filepath = self.output_path / filename
        
        counter = 1
        while filepath.exists():
            filename = f"{title_clean} {counter}.md"
            filepath = self.output_path / filename
            counter += 1
        
        note_content = content
        filepath.write_text(note_content, encoding='utf-8')
        return filepath
    
    def process_email(self, message):
        """Process a single email"""
        message_id = message['id']
        print(f"\nProcessing email: {message_id}")
        
        content = self.get_email_content(message_id)
        
        if content['type'] == 'none':
            print(f"  ✗ No PDF attachment or download link found")
            self.mark_email_processed(message_id)
            return
        
        text = ""
        
        if content['type'] == 'pdf':
            print(f"  ✓ Found PDF attachment: {content['filename']}")
            text = self.extract_text_from_pdf(content['data'])
        
        elif content['type'] == 'link':
            print(f"  ✓ Found {len(content['links'])} download link(s)")
            for link in content['links']:
                print(f"    - Downloading from link...")
                file_data = self.download_from_link(link)
                
                if file_data:
                    if link.endswith('.txt') or b'<!DOCTYPE' not in file_data[:1000]:
                        try:
                            decoded_text = file_data.decode('utf-8', errors='ignore')
                            text += decoded_text + "\n"
                            print(f"    - Extracted text: {len(decoded_text)} chars")
                        except Exception as e:
                            print(f"    - Text decode error: {e}")
                    elif link.endswith('.pdf') or b'%PDF' in file_data[:10]:
                        extracted = self.extract_text_from_pdf(file_data)
                        text += extracted + "\n"
                        print(f"    - Extracted PDF text")
                else:
                    print(f"    - Download failed or returned no data")
        
        if not text.strip():
            print(f"  ✗ No text content extracted")
            self.mark_email_processed(message_id)
            return
        
        print(f"  ✓ Total extracted: {len(text)} characters")
        
        notes = self.parse_highlights_and_notes(text, content['subject'])
        print(f"  ✓ Found {len(notes)} notes/highlights")
        
        created_count = 0
        for note in notes:
            try:
                filepath = self.create_obsidian_note(note)
                created_count += 1
            except Exception as e:
                print(f"  ✗ Error creating note: {e}")
        
        print(f"  ✓ Created {created_count} Obsidian notes")
        self.mark_email_processed(message_id)
    
    def run(self):
        """Main execution"""
        print("=== Kindle Scribe to Obsidian Sync ===\n")
        
        self.authenticate_gmail()
        messages = self.search_kindle_emails()
        
        if not messages:
            print("\n✓ No new Kindle emails to process")
            return
        
        for message in messages:
            try:
                self.process_email(message)
            except Exception as e:
                print(f"✗ Error processing email {message['id']}: {e}")
                import traceback
                traceback.print_exc()
        
        print("\n✓ Sync complete!")

if __name__ == "__main__":
    sync = KindleToObsidian(OBSIDIAN_VAULT_PATH)
    sync.run()