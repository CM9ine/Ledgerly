import pandas as pd
from sqlalchemy import create_engine
import os


class LedgerlyCSVLoader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.engine = self.create_sqlalchemy_engine()

    def create_sqlalchemy_engine(self):
        return create_engine('sqlite:///ledgerly.db')

    def create_db_connection(self):
        return self.engine.connect()

    def close_db_connection(self, connection):
        connection.close()

    def write_transactions_to_db(self, connection, df):
        df.to_sql('transactions', connection, if_exists='replace', index=False)

    def load_csv(self):
        return pd.read_csv(self.file_path)

    def get_monthly_summary(self, df):

        df['date'] = pd.to_datetime(df['date'])

        monthly_summary = (
            df.groupby(df['date'].dt.to_period('M'))
            .agg(total_income=('amount', lambda x: x[df['type'] == 'Income'].sum()),
                total_expenses=('amount', lambda x: x[df['type'] == 'Expense'].sum()))
        )

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, '..', 'data', 'anonymised_bank_data_combined.csv')
    
    loader = LedgerlyCSVLoader(csv_path)
    df = loader.load_csv()
    connection = loader.create_db_connection()
    loader.write_transactions_to_db(connection, df)

    income = df.loc[df['type'] == 'Income', 'amount'].sum()
    expenses = df.loc[df['type'] == 'Expense', 'amount'].sum()
    net = income - expenses

    print(f"Total Income: £{income:,.2f}")
    print(f"Total Expenses: £{expenses:,.2f}")
    print(f"Net: £{net:,.2f}")

    # Ensure date column is datetime
    df['date'] = pd.to_datetime(df['date'])

    monthly_summary = (
        df.groupby(df['date'].dt.to_period('M'))
        .agg(total_income=('amount', lambda x: x[df['type'] == 'Income'].sum()),
            total_expenses=('amount', lambda x: x[df['type'] == 'Expense'].sum()))
    )

    monthly_summary['net'] = monthly_summary['total_income'] - monthly_summary['total_expenses']

    print(monthly_summary)

    
    loader.close_db_connection(connection)