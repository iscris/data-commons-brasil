import pandas as pd

import pandas as pd

def drop_na_in_column(df, column_name):
    """
    Drops rows with NaN values in the specified column of the DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    column_name (str): The name of the column to check for NaN values.

    Returns:
    pandas.DataFrame: The DataFrame with rows dropped that have NaN values in the specified column.
    """
    # Dropping rows with NaN values in the specified column
    cleaned_df = df.dropna(subset=[column_name])
    return cleaned_df


file_list = [
    "metereologia-direcaoVento.csv",
    "metereologia-pressaoAtm.csv",
    "metereologia-umidadeExt.csv",
    "metereologia-precipitacao.csv",
    "metereologia-temperaturaExt.csv",
    "metereologia-velocidadeVento.csv"
]

column_names = ["direcaoVento","pressaoAtm","umidadeExt","precipitacao","temperaturaExt","velocidadeVento"]

for column in column_names:
    df = pd.read_csv("metereologia-"+column+".csv")
    df.rename(columns={'dtHrLeitura': 'date'}, inplace=True)
    new_df = drop_na_in_column(df, column)
    print(len(df), len(new_df))