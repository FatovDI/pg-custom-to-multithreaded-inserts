SET SCHEMA 'test_insertion';

\set id random(203958073, 203959073)

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
  AND paymentdoc.id = :id;
