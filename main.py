from src.email_handler import get_gmail_service, process_email, find_statement_emails
from src.sheets_manager import update_sheet, init_google_sheets

def main():

    try:
        # Autenticações
        gmail_service = get_gmail_service()
        gc = init_google_sheets()

        # Processar emails
        emails = find_statement_emails(gmail_service)

        for email in emails:
            df = process_email(gmail_service, email)
            update_sheet(gc, df, 'Controle Financeiro', 'Transações')

            print(f'Dados atualizados: {len(df)} transações')

    except Exception as e:
        print(f'Erro ao processar emails: {str(e)}')

if __name__ == '__main__':
    main()