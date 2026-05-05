alter table mortgage_records
  alter column loan_amount type bigint,
  alter column original_amount type bigint,
  alter column estimated_current_balance type bigint,
  alter column estimated_monthly_payment type bigint;
