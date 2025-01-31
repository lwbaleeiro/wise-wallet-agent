import os
import re
import base64
import tempfile
import pandas as pd

from pathlib import Path

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from src.csv_processor import parse_nubank_csv, validate_transactions

# ========== Configurações ==========
BASE_DIR = Path(__file__).parent.resolve()
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly',
          'https://www.googleapis.com/auth/gmail.modify']
ATTACHMENT_PATTERN = re.compile(r'NU_\d+_\d{2}[A-Z]{3}\d{4}_\d{2}[A-Z]{3}\d{4}\.csv')

# Caminhos dos arquivos
GMAIL_CREDENTIALS_PATH = BASE_DIR.parent / "config" / "gmail-credentials.json"
TOKEN_PATH = BASE_DIR.parent / "config" / "token.json"

# Configurações específicas do Nubank
SENDER_EMAIL = 'dev.baleeiro@gmail.com'
SUBJECT_FILTER = 'Fwd: Extrato da sua conta do Nubank'

def get_gmail_service():
    creds = None
    
    # Carrega o token exitente
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # Se não há credenciais válidas ou estão expiradas
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(
                port=0,
                authorization_prompt_message='Por favor acesse esta URL: {url}',
                success_message='Autenticação concluída! Você já pode fechar esta janela.',
                open_browser=True                
            )

        # Garante que o diretório existe
        TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)

        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def find_statement_emails(service):
    query = f'from:{SENDER_EMAIL} subject:"{SUBJECT_FILTER}" has:attachment'
    results = service.users().messages().list(
        userId='me',
        q=query,
        labelIds=['INBOX']
    ).execute()
    
    return results.get('messages', [])

def download_attachment(service, message_id, attachment_id, filename):
    try:
        attachment = service.users().messages().attachments().get(
            userId='me',
            messageId=message_id,
            id=attachment_id
        ).execute()

        if 'data' not in attachment:
            raise Exception('Resposta da API não contém dados em anexo.')

        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

        # Cria diretório temporário se não existir
        temp_dir = tempfile.mkdtemp(prefix='nubank_')
        file_path = os.path.join(temp_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(file_data)

        return file_path
    
    except Exception as e:
        print(f"Dados problemáticos: {attachment}")
        raise Exception(f'Error downloading attachment: {str(e)}')

def process_email(service, message):
    try:
        processed_label_id = get_or_create_processed_label(service)
        if not processed_label_id:
            raise Exception('Error creating processed label')
        
        msg = service.users().messages().get(
            userId='me',
            id=message['id'],
            format='full'
        ).execute()

        dataframes = []
        temp_files = []

        for part in msg['payload']['parts']:
            if part['filename'].endswith('.csv') and ATTACHMENT_PATTERN.match(part['filename']):
                attachment_id = part['body']['attachmentId']
                file_path = download_attachment(
                    service,
                    message['id'],
                    attachment_id,
                    part['filename']
                )
                df = parse_nubank_csv(file_path)
                validate_transactions(df)
                temp_files.append(file_path)
        
        combined_df = pd.concat(dataframes, ingonre_index=True) if dataframes else pd.DataFrame()
        
        # Remove arquivos temporários
        for file_path in temp_files:
            try:
                os.remove(file_path)
                os.rmdir(os.path.dirname(file_path))
            except:
                pass

        # Marca email como processado
        service.users().messages().modify(
            userId='me',
            id=message['id'],
            body={
                'addLabelIds': [processed_label_id], 
                'removeLabelIds': ['INBOX']
                }
        ).execute()

        return combined_df
    
    except Exception as e:
        raise Exception(f'Error processing email: {str(e)}')

# Cria label 'Processado' para marcar emails já processados. Necessário ser chamado apenas uma vez
def get_or_create_processed_label(service):
    label_name = "Processado"

    try:
        labels = service.users().labels().list(userId='me').execute().get('labels', [])
        for label in labels:
            if label['name'].lower() == label_name.lower():
                return label['id']
            
        new_label = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show"
        }

        created_label = service.users().labels().create(
            userId='me',
            body=new_label
        ).execute()

        return created_label['id']
         
    except Exception as e:
        print(f'Error creating label: {str(e)}')
        return None

# def main():
#     try:
#         service = get_gmail_service()
#         print('Autenticação bem sucedida!')

#         emails = find_statement_emails(service)

#         for email in emails:
#             try:
#                 attachments = process_email(service, email)
#                 print(f'Arquivos baixados: {attachments}')
#             except Exception as e:
#                 print(f'Error processing attachments email: {str(e)}')

#     except Exception as e:
#         print(f'Error processing email: {str(e)}')

# if __name__ == '__main__':
#     main()