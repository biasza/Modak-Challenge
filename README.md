#Modak-Challenge


## Context

The company provides a feature that allows users to schedule recurring allowances, enabling them to set a specific amount and frequency (e.g., daily, weekly, biweekly, or monthly) for payments they will receive.

Recently, issues were identified in the backend process responsible for updating the allowance and payment schedule tables. These issues led to discrepancies, particularly in the `next_payment_day` fields across the datasets. The data provided reflects all recorded events and the state of the backend tables up to **December 3, 2024**, which should be considered as the current date for analysis.

## Objectives

The objective of this analysis is to investigate the provided datasets and generate a detailed report identifying discrepancies and patterns related to the `next_payment_day` and `payment_date` fields across the backend tables.

### Key Objectives:
- **Identify Discrepancies**:  
  Verify if there are any mismatches between the `next_payment_day` from the `allowance_backend_table` and the `payment_date` in the `payment_schedule_backend_table`.
  
- **Analyze Data Patterns**:  
  Examine recurring issues, such as users having multiple records in the `payment_schedule_backend_table` or incorrect `payment_date` values, and identify potential root causes.

- **Provide Insights**:  
  Highlight potential failures in backend processes that could explain the discrepancies observed.

## Data Availability

The tables in RAW format are available in the repository, allowing for direct analysis and verification of the discrepancies observed.

- [Raw Data Tables Repository](https://gist.github.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4)

## Documentation (high recommended to start here)

- [Documentation](https://github.com/biasza/Modak-Challenge/tree/main/Documentation)