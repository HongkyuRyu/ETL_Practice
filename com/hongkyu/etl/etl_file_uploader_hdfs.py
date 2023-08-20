from com.hongkyu.etl.fred_hdfs import Fred2Hdfs


title_earnings_list = [
    'Average Hourly Earnings of All Employees: Construction in ',
    'Average Hourly Earnings of All Employees: Education and Health Service in ',
    'Average Hourly Earnings of All Employees: Financial Activities in ',
    'Average Hourly Earnings of All Employees: Goods Producing in ',
    'Average Hourly Earnings of All Employees: Leisure and Hospitality in ',
    'Average Hourly Earnings of All Employees: Manufacturing in ',
    'Average Hourly Earnings of All Employees: Private Service Providing in ',
    'Average Hourly Earnings of All Employees: Professional and Business Services in ',
    'Average Hourly Earnings of All Employees: Trade, Transportation, and Utilities in ',
]

title_unemployee = 'Unemployment Rate in '
title_household_income = 'Real Median Household Income in '
title_poverty = 'Estimated Percent of People of All Ages in Poverty for '
title_real_gdp = 'Real Gross Domestic Product: All Industry Total in '
title_tax_exemption = 'Total Tax Exemptions for '
title_labor_force = 'Civilian Labor Force in '

def get_filename(str_title):
    iStart = str_title.index(':') + 2
    iEnd = str_title.index(' in')
    title = str_title[iStart:iEnd].replace(' ', '_').replace(',', '')

    return 'earnings_' + title + '.csv'

if __name__ == '__main__':
    fred = Fred2Hdfs()
    fred.clear_input_files('outputs', 'unemployee_annual.csv')
    df_list = fred.getListFredDF('A', title_unemployee)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('unemployee_annual.csv', df)
        else:
            fred.appendCsvHdfs('unemployee_annual.csv', df)

        df.to_csv('outputs/unemployee_annual.csv', mode='a', index_label='date', header=(i==0))
    print('=============================================Unemployee Annual Done')


