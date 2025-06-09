import smtplib
import ssl
import os
import pandas as pd
import time
import random
import streamlit as st
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders
from datetime import datetime, timedelta
import pytz
from tzlocal import get_localzone
import re
import logging
import threading
import schedule
from dotenv import load_dotenv
import mimetypes

def is_valid_path(file_path):
    """Check if the given value is a valid file path string."""
    return isinstance(file_path, (str, bytes, os.PathLike)) and os.path.exists(file_path)


def read_file_content(file_path):
    """Read content if the given path is a valid text file."""
    if is_valid_path(file_path):
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type and mime_type.startswith("text"):  # Only read text-based files
            with open(file_path, 'r', encoding="utf-8") as f:
                return f.read()
    return None


def attach_file(msg, file_path):
    """Attach a file to the email if the file path is valid."""
    if is_valid_path(file_path):
        with open(file_path, 'rb') as file:
            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                mime_type = "application/octet-stream"

            main_type, sub_type = mime_type.split("/", 1)
            part = MIMEBase(main_type, sub_type)

            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(file_path)}"')
            msg.attach(part)



class EmailAutomationTool:
    def __init__(self):
        # Load environment variables if present
        load_dotenv()
    
        # Initialize session state variables
        if 'scheduled_emails' not in st.session_state:
            st.session_state.scheduled_emails = []
        if 'sent_emails' not in st.session_state:
            st.session_state.sent_emails = []
        if 'recipient' not in st.session_state:
            st.session_state.recipient = ''
        if 'last_check_time' not in st.session_state:
            st.session_state.last_check_time = datetime.now()
        # Initialize saved credentials in session state with different names
        if 'saved_email_sender' not in st.session_state:
            st.session_state.saved_email_sender = ''
        if 'saved_email_password' not in st.session_state:
            st.session_state.saved_email_password = ''
        
        # Email configuration with environment variables as fallback
        self.DEFAULT_EMAIL = os.getenv("EMAIL_ADDRESS", "")
        self.DEFAULT_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
        self.SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        
        # Timezone setup
        self.LOCAL_TIMEZONE = get_localzone()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Scheduler setup
        self.scheduler_running = False
        self.scheduler_thread = None
        self.scheduled_emails_local = []  # Local copy for thread use
        
        # Schedule.py integration
        self.schedule = schedule

    def clear_scheduled_emails(self):
        """Clears all scheduled emails."""
        self.scheduled_emails = []  # Clear the list
        st.success("All scheduled emails have been cleared.")  # Display a success message
    
    def assign_attachment(self, df, selected_file_type, attachment_dir="saved_attachments"):
        """Assigns attachments automatically based on PIN mapping or user upload."""

        if "Attachment" not in df.columns:
            df["Attachment"] = None  

        if selected_file_type == "None":
            df["Attachment"] = df["Attachment"].apply(lambda path: path if path and os.path.exists(str(path)) else None)
            return df  

        if "pin" in df.columns:
            for idx, row in df.iterrows():
                pin = str(row["pin"])
                expected_path = os.path.join(attachment_dir, f"{pin}.{selected_file_type}")

                if os.path.exists(expected_path):
                    df.at[idx, "Attachment"] = expected_path  

        missing_attachments = df["Attachment"].isnull()

        if missing_attachments.any():
            uploaded_file = st.file_uploader("Upload attachment (Optional)", type=["pdf", "docx", "xlsx", "txt", "jpg", "png", "jpeg"])

            if uploaded_file:
                os.makedirs(attachment_dir, exist_ok=True)
                for idx, row in df[missing_attachments].iterrows():
                    pin = str(row["pin"])
                    correct_file_name = f"{pin}.{selected_file_type}"  # ✅ Ensure correct extension
                    file_path = os.path.join(attachment_dir, correct_file_name)

                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    df.at[idx, "Attachment"] = file_path  # ✅ Correctly update path

        return df


    def read_data_file(self, uploaded_file, selected_file_type):
        """Read data from different file formats and auto-assign attachments if PIN column exists"""
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        try:
            # Read CSV, Excel, or TXT files
            if file_extension == 'csv':
                df = pd.read_csv(uploaded_file)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(uploaded_file)
            elif file_extension == 'txt':
                df = pd.read_csv(uploaded_file, sep='\t')
            else:
                st.error(f"Unsupported file format: {file_extension}")
                return None

            df = self.assign_attachment(df, selected_file_type, attachment_dir="saved_attachments")

            st.write("CSV Preview with Attachments")
            st.dataframe(df)  # Display preview in Streamlit
            
            return df
        
        except Exception as e:
            st.error(f"Error processing the file: {e}")
            return None

    def map_attachments_by_pin(self, df, file_type, attachment_dir="saved_attachments"):
        """Map file extensions to PIN numbers in the dataframe"""

        pin_column = next((col for col in df.columns if 'pin' in col.lower()), None)

        # ✅ Debug: Check if the PIN column exists
        print("Columns in DataFrame:", df.columns)
        print("Detected PIN column:", pin_column)

        if not pin_column:
            st.error("No PIN column found in the data. Column name should contain 'pin'.")
            return df

        # ✅ Debug: Print sample PIN values before mapping
        print("Sample PIN values:", df[pin_column].astype(str).head())  

        # Ensure 'Attachment' column exists
        if 'Attachment' not in df.columns:
            df['Attachment'] = ""

        # Start Mapping
        for idx, row in df.iterrows():
            pin = str(row[pin_column]).strip()

            if pin and not pd.isna(pin):
                filename = f"{pin}.{file_type.lower()}"
                filepath = os.path.join(attachment_dir, filename)

                if os.path.exists(filepath):
                    df.at[idx, 'Attachment'] = str(filepath)  # Save the filepath
                else:
                    print(f"File NOT FOUND: {filename} at {filepath}")  # ✅ Debug file existence

        # ✅ Debug: Print DataFrame after attachment mapping
        print("DataFrame After Mapping Attachments:")
        print(df[[pin_column, "Attachment"]].head())  

        return df


    def process_attachments(self, df, file_type, attachment_dir="saved_attachments"):
        """Assigns correct file extensions to attachments based on PIN mapping."""
        
        # Ensure attachment_dir exists
        os.makedirs(attachment_dir, exist_ok=True)  # Create directory if it doesn't exist
        
        # Ensure 'Attachment' column exists
        if "Attachment" not in df.columns:
            df["Attachment"] = None  

        # Ensure file_type is valid
        if not file_type or file_type == "None":
            return df  # No processing needed if no file type selected

        # Ensure attachment_dir exists
        os.makedirs(attachment_dir, exist_ok=True)

        if "pin" in df.columns:
            for idx, row in df.iterrows():
                pin = str(row["pin"]).strip()  # Ensure PIN is a string
                expected_path = os.path.join(attachment_dir, f"{pin}.{file_type}")

                if os.path.exists(expected_path):
                    df.at[idx, "Attachment"] = expected_path
                else:
                    df.at[idx, "Attachment"] = None  # Avoid assigning wrong paths

        return df




    def validate_email(self, email):
        """Validate email address format"""
        # Skip validation for empty or None values
        if not email or pd.isna(email):
            return False
        
        # Convert to string if not already
        if not isinstance(email, str):
            email = str(email)
            
        # Trim whitespace
        email = email.strip()
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_regex, email) is not None

    def validate_time_format(self, time_str):
        """Validate time is in 24-hour format"""
        try:
            # Attempt to parse the time string
            datetime.strptime(time_str, '%H:%M')
            return True
        except ValueError:
            return False

    def parse_schedule_time(self, schedule_date, schedule_time_str):
        """Parse and validate scheduling time"""
        # Validate time format
        if not self.validate_time_format(schedule_time_str):
            st.error("Invalid time format. Please use HH:MM (24-hour format)")
            return None

        try:
            # Parse time
            hours, minutes = map(int, schedule_time_str.split(':'))
            
            # Combine date and time
            schedule_datetime = datetime.combine(
                schedule_date, 
                datetime.min.time().replace(hour=hours, minute=minutes)
            ).replace(tzinfo=self.LOCAL_TIMEZONE)

            # Return the time without adjusting
            return schedule_datetime

        except Exception as e:
            st.error(f"Error parsing schedule time: {e}")
            return None
    def schedule_email(self, email_sender, password, recipients, subject, body, schedule_datetime, attachment=None, recurring=False, recurring_days=None):
        """Schedule an email for future sending"""
        # Prepare email details for scheduling
        # ✅ Ensure attachment paths are saved properly  
        if isinstance(attachment, st.runtime.uploaded_file_manager.UploadedFile):  
            save_dir = "saved_attachments"  
            os.makedirs(save_dir, exist_ok=True)  # ✅ Ensure folder exists  
            saved_path = os.path.join(save_dir, attachment.name)  

            with open(saved_path, "wb") as f:  
                f.write(attachment.getbuffer())  # ✅ Save the uploaded file  

            attachment = saved_path  # ✅ Store the saved path  

        # ✅ Store the correct attachment path  
        email_details = {  
            'sender': email_sender,  
            'password': password,  
            'recipients': recipients,  
            'subject': subject,  
            'body': body,  
            'schedule_time': schedule_datetime,  
            'attachment': attachment,  # ✅ Save the file path instead of UploadedFile  
            'scheduled_at': datetime.now(self.LOCAL_TIMEZONE),  
            'sent': False,  
            'recurring': recurring,  
            'recurring_days': recurring_days,  
            'id': len(st.session_state.scheduled_emails) + 1  
        }  

        
        # Add to scheduled emails in session state
        st.session_state.scheduled_emails.append(email_details)
        # Update local copy for thread safety
        self.scheduled_emails_local = list(st.session_state.scheduled_emails)
        
        # If scheduling for now or the past, send immediately
        now = datetime.now(self.LOCAL_TIMEZONE)
        if schedule_datetime <= now and not recurring:
            try:
                if isinstance(recipients, str):  # Single email case
                    recipients = pd.DataFrame({'Email': [recipients], 'Subject': [subject], 'Body': [body], 'Attachment': [attachment]})
                self.send_bulk_emails(
                    email_sender, 
                    password, 
                    recipients, 
                    subject, 
                    body, 
                    attachment
                )
                
                # Update scheduled email as sent
                for email in st.session_state.scheduled_emails:
                    if email == email_details:
                        email['sent'] = True
                
                # Update local copy
                self.scheduled_emails_local = list(st.session_state.scheduled_emails)
                
                # Add to sent emails
                sent_email = email_details.copy()
                sent_email['sent_at'] = now
                st.session_state.sent_emails.append(sent_email)
                
                st.success(f"Email sent immediately as scheduled time was {schedule_datetime.strftime('%Y-%m-%d %H:%M %Z')}")
            except Exception as e:
                st.error(f"Error sending immediate email: {e}")
        else:
            # Log the scheduling
            self.logger.info(f"Email scheduled. Recipients: {recipients}, Time: {schedule_datetime}")
            
            # If using schedule library and it's recurring
            if recurring:
                self.schedule_recurring_email(email_details)
            
            # Provide user feedback
            if recurring:
                recurring_msg = f" (recurring on {', '.join(recurring_days)})" if recurring_days else " (recurring daily)"
                st.success(f"Email scheduled for {schedule_datetime.strftime('%Y-%m-%d %H:%M %Z')}{recurring_msg}")
            else:
                st.success(f"Email scheduled for {schedule_datetime.strftime('%Y-%m-%d %H:%M %Z')}")
            
            # Ensure the scheduler is running
            self.ensure_scheduler_running()

    def check_and_send_due_emails(self):
        """Check and send only due emails, ensuring no duplicate sends."""
        now = datetime.now(self.LOCAL_TIMEZONE)
        emails_to_send = []
        emails_sent = []  # Track successfully sent emails

        # Ensure scheduled jobs are executed
        self.schedule.run_pending()

        # Copy scheduled emails to prevent modification during iteration
        scheduled_emails_copy = list(self.scheduled_emails_local)

        for email in scheduled_emails_copy:
            schedule_time = email['schedule_time']

            # Ensure schedule_time has a timezone
            if schedule_time.tzinfo is None:
                schedule_time = schedule_time.replace(tzinfo=self.LOCAL_TIMEZONE)

            # Ensure the email is due and not already sent
            if now >= schedule_time and not email.get('sent', False):
                emails_to_send.append(email)

        # Send each due email and mark it as sent
        for email in emails_to_send:
            try:
                self.logger.info(f"Sending email scheduled for {email['schedule_time']} at {now}")

                # ✅ Ensure we correctly retrieve the saved attachment path  
                attachment = email.get("attachment", "")  

                # ✅ Debug print  
                if attachment and not os.path.exists(attachment):  
                    print(f"⚠️ Scheduled attachment file not found: {attachment}")  
                    attachment = None  # ✅ Prevent broken attachment paths  

                self.send_bulk_emails(
                    email['sender'],
                    email['password'],
                    email['recipients'],
                    email['subject'],
                    email['body'],
                    attachment  # ✅ Use the corrected attachment path
                )


                # Mark email as sent in local copy
                email['sent'] = True
                emails_sent.append(email['id'])

                # Update session state to avoid duplicate sending
                if 'scheduled_emails' in st.session_state:
                    for scheduled_email in st.session_state.scheduled_emails:
                        if scheduled_email['id'] == email['id']:
                            scheduled_email['sent'] = True

                self.logger.info(f"Email sent successfully to {email['recipients']}")

            except Exception as e:
                self.logger.error(f"Error sending scheduled email: {e}")

        return emails_sent  # Return list of sent emails


    def scheduler_loop(self):
        """Scheduler loop that ensures each email is sent only once."""
        self.logger.info("Email scheduler started")

        while self.scheduler_running:
            try:
                # Sync local email list from session state
                if 'scheduled_emails' in st.session_state:
                    self.scheduled_emails_local = list(st.session_state.scheduled_emails)

                now = datetime.now(self.LOCAL_TIMEZONE)

                # Identify pending emails that are NOT sent
                pending_emails = [
                    email for email in self.scheduled_emails_local
                    if not email.get('sent', False)
                ]

                if pending_emails:
                    # Find the soonest email that needs to be sent
                    next_email_time = min(email['schedule_time'] for email in pending_emails)

                    # Ensure proper timezone handling
                    if next_email_time.tzinfo is None:
                        next_email_time = next_email_time.replace(tzinfo=self.LOCAL_TIMEZONE)

                    time_until_due = max((next_email_time - now).total_seconds(), 0)

                    # Sleep until the exact email time to avoid extra checks
                    if 0 < time_until_due < 30:
                        time.sleep(time_until_due)
                        emails_sent = self.check_and_send_due_emails()  # Ensure only due emails are sent

                        if emails_sent:
                            self.logger.info(f"Sent {len(emails_sent)} due emails.")
                        continue  # Skip unnecessary sleep

                # Run check and send only if necessary
                emails_sent = self.check_and_send_due_emails()

                if emails_sent:
                    self.logger.info(f"Processed {len(emails_sent)} scheduled emails.")

                # Optimize sleep time (sleep until next email is due)
                next_check_time = min(30, time_until_due)  # Sleep until next due email or 30 seconds max
                time.sleep(max(next_check_time, 5))  # Ensure at least 5 seconds delay

            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(10)  # Avoid infinite retry loops


    def ensure_scheduler_running(self):
            """Ensure the scheduler thread is running"""
            if not self.scheduler_running or (self.scheduler_thread and not self.scheduler_thread.is_alive()):
                # Update local copy from session state
                self.scheduled_emails_local = list(st.session_state.scheduled_emails)
                
                self.scheduler_running = True
                self.scheduler_thread = threading.Thread(target=self.scheduler_loop, daemon=True)
                self.scheduler_thread.start()
                self.logger.info("Started email scheduler thread")


    def normalize_dataframe(self, df):
        """Normalize column names in the dataframe to support various formats"""
        # Create a copy to avoid modifying the original
        normalized_df = df.copy()
        
        # Map of possible column names to standardized names
        column_map = {
            'email': 'Email',
            'recipient': 'Email',
            'receipient': 'Email',
            'recipients': 'Email',
            'to': 'Email',
            'subject': 'Subject',
            'body': 'Body',
            'message': 'Body',
            'content': 'Body',
        }
        
        # Normalize column names (case-insensitive)
        for col in df.columns:
            lower_col = col.lower()
            if lower_col in column_map:
                normalized_df.rename(columns={col: column_map[lower_col]}, inplace=True)
                
        return normalized_df


    def send_bulk_emails(self, email_sender, password, recipients, subject=None, body=None, attachment=None):
        """Send bulk emails with attachment handling, error logging, and batch management."""

        processed_emails = set()

        try:
            context = ssl.create_default_context()
            sent_count = 0
            batch_size = 45  # Sending in batches of 45 emails
            errors = []

            # Debugging log: Check total number of recipients
            print(f"Total recipients to send: {len(recipients)}")

            if not isinstance(recipients, pd.DataFrame):
                recipients = pd.DataFrame({'Email': [recipients], 'Subject': [subject], 'Body': [body], 'Attachment': [attachment]})
            else:
                recipients = recipients[recipients['Email'].apply(lambda x: self.validate_email(x))]

            # Ensure the "Attachment" column is properly processed
            if "Attachment" not in recipients.columns:
                recipients["Attachment"] = None  # Ensure column exists

            recipients["Attachment"] = recipients["Attachment"].fillna("").astype(str).apply(lambda x: x.strip())

            # Debug: Print DataFrame before sending emails
            print("✅ Final Recipients DataFrame Before Sending Emails:")
            print(recipients[["Email", "Attachment"]].head())

            for batch_start in range(0, len(recipients), batch_size):
                batch_end = min(batch_start + batch_size, len(recipients))
                batch = recipients.iloc[batch_start:batch_end]
                print(f"Sending batch {batch_start}-{batch_end} of {len(recipients)} emails")

                with smtplib.SMTP(self.SMTP_SERVER, self.SMTP_PORT) as server:
                    server.starttls(context=context)
                    server.login(email_sender, password)

                    for _, row in batch.iterrows():
                        recipient_email = row['Email'].strip()

                        try:
                            msg = MIMEMultipart()
                            msg['From'] = email_sender
                            msg['To'] = recipient_email
                            processed_emails.add(recipient_email)

                            current_subject = row.get('Subject', subject or "")
                            current_body = row.get('Body', body or "")
                            current_attachment = row.get("Attachment", "").strip()

                            # Handle Streamlit UploadedFile objects correctly
                            if isinstance(current_attachment, st.runtime.uploaded_file_manager.UploadedFile):
                                save_dir = "saved_attachments"
                                os.makedirs(save_dir, exist_ok=True)
                                saved_path = os.path.join(save_dir, current_attachment.name)

                                with open(saved_path, "wb") as f:
                                    f.write(current_attachment.getbuffer())

                                current_attachment = saved_path  # Assign correct path

                            # Debug print to check if the file exists
                            if current_attachment and not os.path.exists(current_attachment):
                                print(f"⚠️ Attachment file not found: {current_attachment}")
                                current_attachment = None  # Prevents broken attachment paths

                            msg['Subject'] = str(current_subject)
                            msg.attach(MIMEText(str(current_body), 'plain'))

                            # Attach file only if it exists
                            if current_attachment:
                                with open(current_attachment, "rb") as f:
                                    attachment_part = MIMEApplication(f.read(), Name=os.path.basename(current_attachment))
                                    attachment_part["Content-Disposition"] = f'attachment; filename="{os.path.basename(current_attachment)}"'
                                    msg.attach(attachment_part)

                            server.send_message(msg)
                            sent_count += 1
                            self.logger.info(f"Email sent to {recipient_email}")
                            time.sleep(0.5)

                        except Exception as e:
                            errors.append(f"Error sending to {recipient_email}: {str(e)}")
                            self.logger.error(f"Error sending to {recipient_email}: {e}")
                            continue

            self.logger.info(f"Successfully sent {sent_count} out of {len(recipients)} emails.")
            print(f"✅ Successfully sent {sent_count} out of {len(recipients)} emails.")

            if errors:
                return f"Sent {sent_count} emails with {len(errors)} errors. First error: {errors[0]}"
            else:
                return f"All {sent_count} emails sent successfully"

        except Exception as e:
            self.logger.error(f"Bulk sending error: {e}")
            print(f"⚠️ Bulk sending error: {e}")
            raise Exception(f"Error sending emails: {e}")





    def process_from_csv(self, file_upload, selected_file_type, send_time=None, recurring=False, recurring_days=None):
        """Import and schedule emails from a CSV file."""
        try:
            df = self.read_data_file(file_upload)
            if df is None:
                return False
                
            df = self.normalize_dataframe(df)

            if 'Email' not in df.columns:
                st.error("File must contain an 'Email' column")
                return False

            valid_emails = df[df['Email'].apply(self.validate_email)]
            if valid_emails.empty:
                st.error("No valid email addresses found in the file")
                return False

            email_sender = st.session_state.saved_email_sender or self.DEFAULT_EMAIL
            password = st.session_state.saved_email_password or self.DEFAULT_PASSWORD

            if not email_sender or not password:
                st.error("Please configure your email credentials first")
                return False

            attachment_dir = "saved_attachments"  # Define attachment directory

            # Make sure the attachment directory exists
            os.makedirs(attachment_dir, exist_ok=True)

            # Process Attachments (No PIN-based Mapping)
            df = self.process_attachments(df, file_type=selected_file_type, attachment_dir=attachment_dir)

            # Ensure valid attachment paths
            df["Attachment"] = df["pin"].apply(
                lambda pin: os.path.join(attachment_dir, f"{pin}.{selected_file_type}") 
                if os.path.exists(os.path.join(attachment_dir, f"{pin}.{selected_file_type}")) else df["Attachment"]
            )



            if send_time:
                hours, minutes = map(int, send_time.split(':'))
                schedule_time = datetime.now().replace(
                    hour=hours, 
                    minute=minutes, 
                    second=0, 
                    microsecond=0,
                    tzinfo=self.LOCAL_TIMEZONE
                )

                if schedule_time < datetime.now(self.LOCAL_TIMEZONE):
                    schedule_time += timedelta(days=1)

                self.schedule_email(
                    email_sender,
                    password,
                    valid_emails,
                    df.get('Subject', "No Subject").iloc[0] if 'Subject' in df else "No Subject",
                    df.get('Body', "No Message").iloc[0] if 'Body' in df else "No Message",
                    schedule_time,
                    None,  
                    recurring,
                    recurring_days
                )

                return True
            else:
                if 'ScheduleTime' not in df.columns:
                    st.error("File must contain a 'ScheduleTime' column")
                    return False

                for _, row in valid_emails.iterrows():
                    time_str = row['ScheduleTime']
                    if not self.validate_time_format(time_str):
                        continue

                    hours, minutes = map(int, time_str.split(':'))
                    schedule_time = datetime.now().replace(
                        hour=hours, 
                        minute=minutes, 
                        second=0, 
                        microsecond=0,
                        tzinfo=self.LOCAL_TIMEZONE
                    )

                    if schedule_time < datetime.now(self.LOCAL_TIMEZONE):
                        schedule_time += timedelta(days=1)

                    self.schedule_email(
                        email_sender,
                        password,
                        pd.DataFrame([row]),
                        row.get('Subject', "No Subject"),
                        row.get('Body', "No Message"),
                        schedule_time,
                        None,  
                        recurring,
                        recurring_days
                    )

                return True

        except Exception as e:
            st.error(f"Error processing file: {e}")
            return False


    def main_interface(self):
        """Main Streamlit interface"""
        st.title("Email Automation Tool")

        # Ensure session state variables are initialized
        if "scheduled_emails" not in st.session_state:
            st.session_state.scheduled_emails = []
        if "sent_emails" not in st.session_state:
            st.session_state.sent_emails = []
        if "last_check_time" not in st.session_state:
            st.session_state.last_check_time = datetime.now()

        # Ensure scheduler is running if pending emails exist
        if any(not email.get("sent", False) for email in st.session_state.scheduled_emails):
            self.ensure_scheduler_running()

        # Check and send emails only if interval has passed
        current_time = datetime.now()
        check_interval = 60  # seconds

        if (current_time - st.session_state.last_check_time).total_seconds() > check_interval:
            st.session_state.last_check_time = current_time

            if any(not email.get("sent", False) for email in st.session_state.scheduled_emails):
                emails_sent = self.check_and_send_due_emails()  # FIXED: No unpacking issue

                if emails_sent:
                    for email in st.session_state.scheduled_emails:
                        if not email.get("sent", False):
                            email["sent"] = True
                            sent_email = email.copy()
                            sent_email["sent_at"] = datetime.now(self.LOCAL_TIMEZONE)
                            st.session_state.sent_emails.append(sent_email)

                    st.success("Processed scheduled emails that were due")
                    time.sleep(1)
                    st.rerun()

        file_types = ["None", "pdf", "pptx", "docx", "xlsx", "jpg", "png", "jpeg"]
        selected_file_type = st.radio("Select File Type for Attachments", file_types, index=0)

        # Troubleshooting section
        with st.expander("Troubleshooting & Debug", expanded=False):
            st.write(f"Current time: {datetime.now(self.LOCAL_TIMEZONE)}")
            st.write(f"Last check time: {st.session_state.last_check_time}")
            st.write(f"Scheduler running: {self.scheduler_running}")
            st.write(f"One-time scheduled emails: {len([e for e in st.session_state.scheduled_emails if not e.get('recurring', False) and not e.get('sent', False)])}")
            st.write(f"Recurring scheduled emails: {len([e for e in st.session_state.scheduled_emails if e.get('recurring', False)])}")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("Check For Due Emails Now"):
                    emails_sent = self.check_and_send_due_emails()  # FIXED: No unpacking issue

                    if emails_sent:
                        for email in st.session_state.scheduled_emails:
                            if not email.get("sent", False):
                                email["sent"] = True
                                sent_email = email.copy()
                                sent_email["sent_at"] = datetime.now(self.LOCAL_TIMEZONE)
                                st.session_state.sent_emails.append(sent_email)

                        st.success("Found and sent due emails!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.info("No emails due at this time")

            with col2:
                if st.button("Clear All Scheduled"):
                    self.clear_scheduled_emails()
                    st.rerun()


        # Requirements info
        with st.expander("File Requirements", expanded=False):
            st.markdown("""
            ### File Requirements
            - Supported file formats: CSV, XLSX, XLS, TXT
            - Your file must contain at least one of these column names (case insensitive):
              - **Email**, **email**, **recipient**, or **to**
            - Optional columns:
              - **Subject** - custom subject line for each recipient
              - **Body** - custom message body for each recipient
              - **ScheduleTime** - scheduling time for each email (format: HH:MM)
              - **PIN column** - used for PIN-based attachment mapping
              - **Attachment** - used for attaching files it must be same (Attachment)


            ### PIN-based Attachment Mapping
            - The system looks for a column containing "pin" in its name
            - Files should be named as `PIN.extension` (e.g., `22551A4283.pdf`)
            - The system verifies file existence before mapping

            ### Scheduling Notes
            - Use 24-hour format (HH:MM) for scheduling
            - Recurring emails can be scheduled daily or on specific days
            """)

        # Sidebar Configuration
        st.sidebar.header("Email Configuration")

        # Email credentials input
        email_sender = st.sidebar.text_input("Sender Email", value=st.session_state.saved_email_sender or self.DEFAULT_EMAIL, key="email_sender_input")
        password = st.sidebar.text_input("App Password", value=st.session_state.saved_email_password or self.DEFAULT_PASSWORD, type="password", key="email_password_input")

        # Save credentials option
        if st.sidebar.checkbox("Save credentials for this session"):
            st.session_state.saved_email_sender = email_sender
            st.session_state.saved_email_password = password
            st.sidebar.success("Credentials saved for this session")

        mode = st.sidebar.selectbox("Send Mode", ["Single Email", "Bulk Email"])
        send_type = st.sidebar.selectbox("Send Type", ["Send Immediately", "Schedule Email"])

        # Initialize variables
        recipients = None
        csv_has_subject = False
        csv_has_body = False

        if mode == "Single Email":
            # Use session state to persist recipient
            recipient = st.text_input("Recipient Email", value=st.session_state.recipient, key="recipient_input")

            # Update session state
            st.session_state.recipient = recipient
            recipients = recipient

            # Single email needs subject and body
            subject = st.text_input("Enter Email Subject", value="", placeholder="(Optional)")
            body = st.text_area("Email Body", key="email_body")


        else:  # Bulk Email
            bulk_upload_file = st.file_uploader("Upload Bulk Email Data", type=['csv', 'xlsx', 'xls', 'txt'], key="file_uploader_bulk")

            if bulk_upload_file:
                try:
                    recipients = self.read_data_file(bulk_upload_file, selected_file_type)  # ✅ Updated to pass file type

                    if recipients is not None:
                        recipients = self.normalize_dataframe(recipients)

                        # ✅ Auto-assign attachments based on PINs if file type is selected
                        if 'Attachment' not in recipients.columns:  # Check if attachment column exists
                            if 'pin' in recipients.columns and selected_file_type != "None":
                                recipients["Attachment"] = recipients["pin"].apply(
                                    lambda pin: os.path.join("attachments", f"{pin}.{selected_file_type}") 
                                    if os.path.exists(os.path.join("attachments", f"{pin}.{selected_file_type}")) else None
                                )
                            else:
                                recipients["Attachment"] = None  # Allow manual upload

                        # Check if Email column exists after normalization
                        if 'Email' not in recipients.columns:
                            st.error("File must contain a column named 'Email', 'email', 'recipient', or similar")
                            return


                        # Check if Subject/Body columns exist
                        csv_has_subject = 'Subject' in recipients.columns
                        csv_has_body = 'Body' in recipients.columns

                        # Count valid and invalid emails
                        valid_mask = recipients['Email'].apply(self.validate_email)
                        invalid_count = (~valid_mask).sum()
                        valid_count = valid_mask.sum()

                        # Preview data
                        st.subheader("CSV Preview with Attachments")
                        st.dataframe(recipients.head())  # ✅ Updated preview with attachment column

                        # Show email validation summary
                        st.info(f"Found {valid_count} valid email(s) and {invalid_count} invalid email(s)")

                        # Show invalid emails if any
                        if invalid_count > 0:
                            invalid_emails = recipients[~valid_mask]
                            with st.expander("View Invalid Emails"):
                                st.dataframe(invalid_emails)
                                st.warning("Invalid emails will be skipped during sending")

                except Exception as e:
                    st.error(f"Error reading file: {e}")
                    return


            # Subject and Body inputs for bulk emails (only if not in CSV)
            if not csv_has_subject:
                subject = st.text_input("Enter Email Subject", value="", placeholder="(Optional)")
            else:
                subject = ""  # Will be taken from CSV
                st.success("Using subject lines from CSV")

            if not csv_has_body:
                body = st.text_area("Email Body", key="email_body")
            else:
                body = ""  # Will be taken from CSV
                st.success("Using message bodies from CSV")

        # Attachment Option
        attachment = st.file_uploader("Attachment (optional)", type=['pdf', 'docx', 'xlsx', 'txt', 'jpg', 'png'])

        # Scheduling options if Schedule Email selected
        if send_type == "Schedule Email":
            schedule_col1, schedule_col2 = st.columns(2)

            with schedule_col1:
                schedule_date = st.date_input(
                    "Schedule Date",
                    value=datetime.now().date(),
                    min_value=datetime.now().date()
                )

            with schedule_col2:
                schedule_time = st.text_input(
                    "Schedule Time (HH:MM)",
                    placeholder="Enter time in 24-hour format"
                )

            recurring = st.checkbox("Make this a recurring schedule?")

            if recurring:
                days_options = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                recurring_days = st.multiselect("Select days to repeat", days_options)
            else:
                recurring_days = None

        # Send button and processing
        if st.button("Send Email" if send_type == "Send Immediately" else "Schedule Email"):
            # Validate email sender and password
            if not email_sender or not password:
                st.error("Please enter your email credentials")
                return

            # Validate recipient email
            if mode == "Single Email":
                if not self.validate_email(recipients):
                    st.error("Please enter a valid recipient email address")
                    return
            elif mode == "Bulk Email" and recipients is None:
                st.error("Please upload a CSV file with email addresses")
                return

            # Validate subject and body
            # Allow empty subject and body if not present in CSV or interface
            if not csv_has_subject:
                subject = subject if subject is not None else ""  # Allow empty subject

            if not csv_has_body:
                body = body if body is not None else ""  # Allow empty body


            try:
                if send_type == "Send Immediately":
                    if mode == "Single Email":
                        # ✅ Ensure attachment is correctly handled
                        recipients_df = pd.DataFrame({
                            'Email': [recipients], 
                            'Subject': [subject], 
                            'Body': [body], 
                            'Attachment': [attachment.name if attachment else None]
                        })

                        # ✅ Validate attachment paths
                        recipients_df["Attachment"] = recipients_df["Attachment"].apply(lambda x: x if x and os.path.exists(x) else None)

                        result = self.send_bulk_emails(
                            email_sender,
                            password,
                            recipients_df,  # ✅ Convert single email to DataFrame format
                            subject,
                            body,
                            recipients_df["Attachment"].iloc[0]  # ✅ Ensure valid attachment is passed
                        )

                    else:
                        # Bulk email sending
                        recipients["Attachment"] = recipients["Attachment"].apply(lambda x: x if x and os.path.exists(x) else None)

                        result = self.send_bulk_emails(
                            email_sender,
                            password,
                            recipients,
                            subject,
                            body,
                            None  # ✅ Attachments are handled inside send_bulk_emails()
                        )

                    st.success(result)



                    # Add to sent emails history
                    sent_email = {
                        'sender': email_sender,
                        'recipients': recipients,
                        'subject': subject,
                        'body': body,
                        'sent_at': datetime.now(self.LOCAL_TIMEZONE),
                        'attachment': attachment.name if attachment else None
                    }
                    st.session_state.sent_emails.append(sent_email)

                else:
                    # Schedule email for later
                    if not schedule_time:
                        st.error("Please enter a schedule time")
                        return

                    if not self.validate_time_format(schedule_time):
                        st.error("Invalid time format. Please use HH:MM format")
                        return

                    # Parse schedule time
                    schedule_datetime = self.parse_schedule_time(schedule_date, schedule_time)
                    if schedule_datetime is None:
                        return

                    # Check if scheduled time is in the past
                    now = datetime.now(self.LOCAL_TIMEZONE)
                    if schedule_datetime < now and not recurring:
                        st.warning("Scheduled time is in the past. Email will be sent immediately.")

                    # Schedule the email
                    self.schedule_email(
                        email_sender,
                        password,
                        recipients,
                        subject,
                        body,
                        schedule_datetime,
                        attachment,
                        recurring,
                        recurring_days
                    )

            except Exception as e:
                st.error(f"Error: {e}")

        # Display Scheduled Emails
        if st.session_state.scheduled_emails:
            with st.expander("Scheduled Emails", expanded=True):
                # Show scheduled but not sent emails
                pending_emails = [e for e in st.session_state.scheduled_emails if not e.get('sent', False)]

                # Create a dataframe for display
                if pending_emails:
                    scheduled_df = pd.DataFrame([
                        {
                            'ID': e.get('id', 'N/A'),
                            'Recipients': len(e['recipients']) if isinstance(e['recipients'], pd.DataFrame) else 1,
                            'Subject': e['subject'],
                            'Schedule Time': e['schedule_time'].strftime('%Y-%m-%d %H:%M %Z'),
                            'Recurring': 'Yes' if e.get('recurring', False) else 'No',
                            'Recurring Days': ', '.join(e.get('recurring_days', [])) if e.get('recurring_days') else 'Daily' if e.get('recurring', False) else 'N/A'
                        } for e in pending_emails
                    ])

                    st.dataframe(scheduled_df)

                    if st.button("Refresh Scheduled Emails"):
                        st.rerun()
                else:
                    st.info("No pending scheduled emails")

        # Display Sent Emails History
        if st.session_state.sent_emails:
            with st.expander("Sent Emails History", expanded=False):
                # Sort by sent time (most recent first)
                sent_emails = sorted(
                    st.session_state.sent_emails,
                    key=lambda x: x.get('sent_at', datetime.min.replace(tzinfo=self.LOCAL_TIMEZONE)),
                    reverse=True
                )

                # Create a dataframe for display
                sent_df = pd.DataFrame([
                    {
                        'Recipients': len(e['recipients']) if isinstance(e['recipients'], pd.DataFrame) else 1,
                        'Subject': e['subject'],
                        'Sent At': e.get('sent_at', 'Unknown').strftime('%Y-%m-%d %H:%M %Z') if isinstance(e.get('sent_at'), datetime) else 'Unknown',
                        'Scheduled': 'Yes' if 'schedule_time' in e else 'No'
                    } for e in sent_emails
                ])

                st.dataframe(sent_df)

                if st.button("Clear History"):
                    st.session_state.sent_emails = []
                    st.success("Sent emails history cleared")
                    st.rerun()
        
        # Display instructions and help
        with st.expander("Help & Instructions", expanded=False):
            st.markdown("""
            ### Email Automation Tool Instructions
            
            #### Basic Usage
            1. Enter your email credentials in the sidebar
            2. Choose between single or bulk email mode
            3. Enter recipient(s), subject, and body
            4. Choose to send immediately or schedule for later
            5. Click the Send/Schedule button
            
            #### Scheduling Emails
            - You can schedule emails for a future date and time
            - Set up recurring emails that repeat daily or on specific days
            - View all scheduled emails in the "Scheduled Emails" section
            
            #### Bulk Emails with CSV
            - Create a CSV file with columns for Email, Subject (optional), and Body (optional)
            - Upload the CSV in the "Bulk Email" mode
            - Individual emails with personalized content will be sent to each recipient
            
            #### Gmail Setup
            1. Go to Google Account → Security → App passwords
            2. Generate a new app password for this app
            3. Use this app password instead of your regular Gmail password
            
            #### Troubleshooting
            - If emails are not being sent, check your credentials
            - Ensure less secure app access is enabled for your email provider
            - For Gmail, use an App Password (2FA must be enabled)
            - Check the "Troubleshooting & Debug" section for more information
            """)
        
        # Footer
        st.markdown("---")
        st.caption("Email Automation Tool v1.0.0")

# Run the app
if __name__ == "__main__":
    email_tool = EmailAutomationTool()
    email_tool.main_interface()