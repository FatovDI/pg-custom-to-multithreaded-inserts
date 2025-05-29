SET SCHEMA 'test_insertion';

-- Генерация случайной даты в заданном диапазоне
-- \set date 'date_trunc(''year'', now() - (random() * interval ''2023-12-31'' - ''2020-01-01''::date))::date + random() * interval ''1 year'' - interval ''1 day'''

-- \set dt '2025-01-19'

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
         left outer join active_transaction at on pd.transaction_id = at.transaction_id
where at.transaction_id is null
  AND pd.order_date = '2025-02-03';
