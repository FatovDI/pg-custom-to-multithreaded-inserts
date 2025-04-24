SET SCHEMA 'test_insertion';

select paymentdoc.id,
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
where paymentdoc.ready_to_read = true
  AND paymentdoc.order_date = '2025-01-19';
