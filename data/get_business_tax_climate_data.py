import pandas as pd

if __name__ == '__main__':
    # Get tables from the tax foundation article
    tables = pd.read_html('https://taxfoundation.org/2021-state-business-tax-climate-index/')
    # Get first table, the summary table, and drop the last row, which 
    # contains notes
    df = tables[0]
    df = df.drop(df.tail(1).index)
    # Write to CSV
    df.to_csv('data/tax_climate/state_business_tax_climate.csv', index=False)