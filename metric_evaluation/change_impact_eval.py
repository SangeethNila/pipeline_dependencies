import pandas as pd
from scipy import stats
from cliffs_delta import cliffs_delta

def evaluate_coupling(coupling_matrix_path, cochange_matrix_path):
    # Load the two separate matrices (dataframes)
    # Example of loading two CSV files (replace with your file paths)
    c_matrix = pd.read_csv(coupling_matrix_path, index_col=[0])  # This matrix contains coupling values
    cochange_matrix = pd.read_csv(cochange_matrix_path, index_col=[0])  # This matrix contains cochange percentages

    # Find the common indices (pairs) between the two matrices (both row and column)
    # Here, we are considering both row and column indices for comparison
    common_rows = c_matrix.index.intersection(cochange_matrix.index)
    common_columns = c_matrix.columns.intersection(cochange_matrix.columns)

    # Extract the coupling values and cochange percentages for the common pairs
    c_values_common = c_matrix.loc[common_rows, common_columns].values.flatten()
    cochange_values_common = cochange_matrix.loc[common_rows, common_columns].values.flatten()

    # Create two groups based on coupling values
    group_c_gt_0 = cochange_values_common[c_values_common > 0]
    group_c_eq_0 = cochange_values_common[c_values_common == 0]

    # Perform the Mann-Whitney U test
    statistic, p_value = stats.mannwhitneyu(group_c_gt_0, group_c_eq_0, alternative='greater')

    # Output the results
    print("Mann-Whitney U test statistic:", statistic)
    print("P-value:", p_value)

    # Interpretation of the result
    if p_value < 0.05:
        print("The difference between the two groups is statistically significant.")
    else:
        print("There is no statistically significant difference between the two groups.")

    # Perform the Cliff's Delta test
    delta = cliffs_delta(group_c_gt_0, group_c_eq_0)
    print("Cliff's Delta:", delta)

    tau, tau_p_value = stats.kendalltau(c_values_common, cochange_values_common)

    # Output Kendall's Tau results
    print("Kendall's Tau coefficient:", tau)
    print("Kendall's Tau p-value:", tau_p_value)

    # Interpretation of Kendall's Tau result
    if tau_p_value < 0.05:
        print("There is a statistically significant correlation between coupling values and cochange percentages.")
    else:
        print("There is no statistically significant correlation between coupling values and cochange percentages.")