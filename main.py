from src.email_handler import get_gmail_service, process_email, find_statement_emails, mark_as_processed
from src.sheets_manager import update_sheet, get_sheets_service, save_insights_to_sheet
from src.ai_integration import generate_financial_insights

def main():

    try:
        # Autenticações
        gmail_service = get_gmail_service()
        sheets_service = get_sheets_service()

        # Processar emails
        emails = find_statement_emails(gmail_service)

        for email in emails:
            df = process_email(gmail_service, email)
            update_sheet(sheets_service, df, 'Controle Financeiro', 'Transações')

            insights = generate_financial_insights(df)
            save_insights_to_sheet(sheets_service, insights)
            print('Análises salvas')

            mark_as_processed(gmail_service, email)
            print(f'Dados atualizados: {len(df)} transações')

    except Exception as e:
        print(f'Erro ao processar emails: {str(e)}')

if __name__ == '__main__':
    main()