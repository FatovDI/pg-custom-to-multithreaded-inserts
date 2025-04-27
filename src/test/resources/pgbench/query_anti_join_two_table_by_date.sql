SET SCHEMA 'test_insertion';

-- Генерация случайной даты в заданном диапазоне
-- \set date 'date_trunc(''year'', now() - (random() * interval ''2023-12-31'' - ''2020-01-01''::date))::date + random() * interval ''1 year'' - interval ''1 day'''

-- \set dt '2025-01-19'

select paymentdoc.id,
       paymentdoc.transaction_id,
       paymentdoc.account_id,
       paymentdoc.amount,
       paymentdoc.cur,
       paymentdoc.expense,
       paymentdoc.order_date,
       paymentdoc.order_number,
       paymentdoc.payment_purpose,
       paymentdoc.prop_10,
       paymentdoc.prop_15,
       paymentdoc.prop_20
from payment_document paymentdoc
where (NOT EXISTS(SELECT * FROM active_transaction at WHERE at.transaction_id = paymentdoc.transaction_id))
  AND paymentdoc.order_date = '2025-02-03';
