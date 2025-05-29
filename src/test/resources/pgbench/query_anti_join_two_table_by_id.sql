SET SCHEMA 'test_insertion';

\set id random(203958073, 203959073)

select pd.id,
       pd.transaction_id,
       pd.account_id,
       pd.amount,
       pd.cur,
       pd.expense,
       pd.order_date,
       pd.order_number,
       pd.payment_purpose,
       pd.prop_10,
       pd.prop_15,
       pd.prop_20
from payment_document pd
where (NOT EXISTS(SELECT * FROM active_transaction at WHERE at.transaction_id = pd.transaction_id))
  AND pd.id = :id;
